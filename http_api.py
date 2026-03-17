"""
HTTP API + RabbitMQ bridge.

Вход:  RabbitMQ queue 'batch_input'  (внешний продюсер пишет сюда)
Выход: RabbitMQ queue 'project_results' (внешний консьюмер читает отсюда)
Внутри: in-memory очереди между сервисами pipeline

HTTP API на порту 8000 — для тестирования:
  POST /send    — отправить batch (проксирует в RabbitMQ batch_input)
  GET  /results — прочитать результаты (буфер из project_results)
  GET  /health  — health check
"""
import json
import logging
import os
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

# In-memory транспорт внутри pipeline
os.environ.setdefault("TRANSPORT_MODE", "in_memory")

from shared.config.settings import queue_names
from shared.messaging.rabbitmq_client import RabbitClient
from shared.messaging.transport import Transport

from services.splitter_service.main import run as run_splitter
from services.prefilter_service.main import run as run_prefilter
from services.dedup_service.main import run as run_dedup
from services.embedding_service.main import run as run_embedding
from services.clustering_service.main import run as run_clustering
from services.nlp_service.main import run as run_nlp
from services.aggregator_service.main import run as run_aggregator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_QUEUE = "batch_input"

# Буфер результатов для HTTP /results
_results = []
_results_lock = threading.Lock()


# ──────────────────────────────────────────────
# RabbitMQ → in-memory bridge (вход)
# Читает из RabbitMQ batch_input, пишет в in-memory batch_input
# ──────────────────────────────────────────────
def rabbit_input_bridge():
    """Consume from RabbitMQ batch_input → publish to in-memory batch_input."""
    client = RabbitClient()
    client.declare_queue(BATCH_QUEUE)

    def on_batch(ch, method, properties, body):
        payload = json.loads(body)
        logger.info("RabbitMQ→pipeline: projectId=%s posts=%d",
                    payload.get("projectId"), len(payload.get("posts", [])))
        Transport._in_memory_instance.publish(BATCH_QUEUE, payload)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    thread = client.consume(BATCH_QUEUE, on_batch)
    thread.join()


# ──────────────────────────────────────────────
# in-memory → RabbitMQ bridge (выход)
# Читает из in-memory project_results, пишет в RabbitMQ project_results
# ──────────────────────────────────────────────
def rabbit_output_bridge():
    """Consume from in-memory project_results → publish to RabbitMQ project_results."""
    rabbit = RabbitClient()
    rabbit.declare_queue(queue_names.project_results)

    def on_result(ch, method, properties, body):
        payload = json.loads(body)
        logger.info("pipeline→RabbitMQ: projectId=%s posts=%d",
                    payload.get("projectId"), len(payload.get("posts", [])))
        rabbit.publish(queue_names.project_results, payload)
        # Также сохраняем в HTTP буфер для /results
        with _results_lock:
            _results.append(payload)

    Transport._in_memory_instance.consume(queue_names.project_results, on_result)
    while True:
        time.sleep(1)


# ──────────────────────────────────────────────
# HTTP API (для тестирования)
# ──────────────────────────────────────────────
class PipelineHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/send':
            content_length = int(self.headers['Content-Length'])
            body = self.rfile.read(content_length)
            payload = json.loads(body.decode())

            # Публикуем напрямую в RabbitMQ batch_input
            _http_rabbit_publisher.publish(BATCH_QUEUE, payload)
            logger.info("HTTP→RabbitMQ: projectId=%s posts=%d",
                        payload.get("projectId"), len(payload.get("posts", [])))

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "sent"}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "healthy"}).encode())
        elif self.path == '/results':
            with _results_lock:
                results_copy = list(_results)
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"results": results_copy}, ensure_ascii=False).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        logger.info("HTTP: %s", args[0])


def run_server(port=8000):
    logger.info("=" * 60)
    logger.info("  ML PIPELINE + RABBITMQ BRIDGE")
    logger.info("=" * 60)

    # 1. Создаём in-memory Transport singleton
    transport = Transport()
    transport.declare_queue(BATCH_QUEUE)
    logger.info("In-memory transport ready")

    # 2. Запускаем pipeline сервисы (in-memory внутри)
    service_threads = [
        threading.Thread(target=run_splitter, name="splitter", daemon=True),
        threading.Thread(target=run_prefilter, name="prefilter", daemon=True),
        threading.Thread(target=run_dedup, name="dedup", daemon=True),
        threading.Thread(target=run_embedding, name="embedding", daemon=True),
        threading.Thread(target=run_clustering, name="clustering", daemon=True),
        threading.Thread(target=run_nlp, name="nlp", daemon=True),
        threading.Thread(target=run_aggregator, name="aggregator", daemon=True),
    ]
    for t in service_threads:
        t.start()
        logger.info("Started: %s", t.name)

    # 3. RabbitMQ→pipeline bridge (вход)
    threading.Thread(target=rabbit_input_bridge, name="rabbit-input", daemon=True).start()
    logger.info("RabbitMQ input bridge started (queue: %s)", BATCH_QUEUE)

    # 4. pipeline→RabbitMQ bridge (выход)
    threading.Thread(target=rabbit_output_bridge, name="rabbit-output", daemon=True).start()
    logger.info("RabbitMQ output bridge started (queue: %s)", queue_names.project_results)

    logger.info("=" * 60)
    logger.info("  Ready. Listening on port %d", port)
    logger.info("  IN:  RabbitMQ '%s'", BATCH_QUEUE)
    logger.info("  OUT: RabbitMQ '%s'", queue_names.project_results)
    logger.info("=" * 60)

    # 5. HTTP сервер для тестирования
    server = HTTPServer(('0.0.0.0', port), PipelineHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        server.shutdown()


# RabbitMQ клиент для HTTP /send
_http_rabbit_publisher = RabbitClient()

if __name__ == "__main__":
    run_server()
