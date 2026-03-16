import json
import logging
import time


from shared.config.settings import queue_names
from shared.models.rubert_model import RuBertModel
from shared.messaging.transport import Transport
from shared.monitoring.metrics import PipelineMetrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

metrics = PipelineMetrics("nlp")


def _format_output(payload: dict, nlp_result: dict) -> dict:
    return {
        "clusterId": payload.get("cluster_id"),
        "projectId": payload.get("projectId"),
        "total_posts": payload.get("total_posts", 1),  # Пробрасываем для агрегатора
        "posts": [
            {
                "title": payload.get("title", ""),
                "content": payload.get("content", "")[:500],
                "type": payload.get("type", ""),
                "url": payload.get("url", ""),
                "metrics": {
                    "relevancy": payload.get("relevancy", 0),
                    "relevancy_score": round(payload.get("relevancy", 0) / 100, 2),
                    "sentiment": nlp_result.get("sentiment_label", "neutral"),
                    "sentiment_score": round(nlp_result.get("sentiment_score", 0.5), 2),
                },
            }
        ],
    }


def _make_handler(model, transport):
    def _handle_message(ch, method, properties, body):
        t0 = time.time()
        payload = json.loads(body)
        text = payload.get("content", "")
        try:
            result = model.predict(text)
        except Exception:
            logger.warning("NLP model failed, using fallback (unknown tone)")
            result = {"sentiment_label": "unknown", "sentiment_score": 0.5, "confidence": 0.0}
        output = _format_output(payload, result)
        transport.publish(queue_names.analysis, output)
        latency = ""
        ts = payload.get("timestamp")
        if ts:
            latency = f" latency={time.time() - ts:.2f}s"
        metrics.inc("messages_processed")
        metrics.observe_latency(time.time() - t0)
        logger.info("NLP: post %s -> cluster %s sentiment=%s relevancy=%s%s",
                     payload.get("post_id"), output["clusterId"],
                     output["posts"][0]["metrics"]["sentiment"],
                     output["posts"][0]["metrics"]["relevancy"],
                     latency)
        if hasattr(ch, 'basic_ack'):
            ch.basic_ack(delivery_tag=method.delivery_tag)
    return _handle_message


def run() -> None:
    client = Transport()
    model = RuBertModel()
    client.declare_queue(queue_names.clustered, dlq_name=queue_names.clustered_dlq)
    client.declare_queue(queue_names.analysis)
    thread = client.consume(queue_names.clustered, _make_handler(model, client),
                            dlq_name=queue_names.clustered_dlq)
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("NLP service stopping")
    finally:
        client.close()


if __name__ == "__main__":
    run()
