"""
Single-container entrypoint — запускает весь pipeline в одном процессе.

Все сервисы работают в отдельных потоках, общаются через in-memory очереди.
"""
import logging
import signal
import sys
import threading
import time

# Устанавливаем TRANSPORT_MODE до импорта сервисов
import os
os.environ["TRANSPORT_MODE"] = "in_memory"

from services.splitter_service.main import run as run_splitter
from services.prefilter_service.main import run as run_prefilter
from services.dedup_service.main import run as run_dedup
from services.embedding_service.main import run as run_embedding
from services.clustering_service.main import run as run_clustering
from services.nlp_service.main import run as run_nlp
from services.aggregator_service.main import run as run_aggregator
from shared.messaging.transport import Transport

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("  ML PIPELINE — SINGLE CONTAINER MODE")
    logger.info("=" * 60)
    logger.info("  Transport: in-memory queues")
    logger.info("  Services: splitter, prefilter, dedup, embedding, clustering, nlp, aggregator")
    logger.info("=" * 60)

    # Запускаем все сервисы в отдельных потоках
    threads = [
        threading.Thread(target=run_splitter, name="splitter", daemon=True),
        threading.Thread(target=run_prefilter, name="prefilter", daemon=True),
        threading.Thread(target=run_dedup, name="dedup", daemon=True),
        threading.Thread(target=run_embedding, name="embedding", daemon=True),
        threading.Thread(target=run_clustering, name="clustering", daemon=True),
        threading.Thread(target=run_nlp, name="nlp", daemon=True),
        threading.Thread(target=run_aggregator, name="aggregator", daemon=True),
    ]

    for thread in threads:
        thread.start()
        logger.info("Started service thread: %s", thread.name)

    logger.info("=" * 60)
    logger.info("  All services started. Waiting for messages...")
    logger.info("=" * 60)

    # Graceful shutdown
    shutdown_event = threading.Event()

    def signal_handler(sig, frame):
        logger.info("Shutdown signal received, stopping services...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Ждём сигнала остановки
    try:
        shutdown_event.wait()
    except KeyboardInterrupt:
        pass

    logger.info("Shutting down...")

    # Закрываем транспорт (это остановит consumer threads)
    Transport._in_memory_instance.shutdown(timeout=5.0) if Transport._in_memory_instance else None

    # Ждём завершения потоков
    for thread in threads:
        thread.join(timeout=2.0)
        if thread.is_alive():
            logger.warning("Thread %s did not stop gracefully", thread.name)

    logger.info("Pipeline stopped.")


if __name__ == "__main__":
    main()
