import json
import logging
import time

import pika


from shared.config.settings import queue_names
from shared.models.rubert_model import RuBertModel
from shared.messaging.rabbitmq_client import RabbitClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _format_output(payload: dict, nlp_result: dict) -> dict:
    return {
        "clusterId": payload.get("cluster_id"),
        "projectId": payload.get("projectId"),
        "posts": [
            {
                "title": payload.get("title", ""),
                "content": payload.get("content", "")[:500],
                "type": payload.get("type", ""),
                "url_string": payload.get("url_string", ""),
                "metrics": {
                    "relevancy": payload.get("relevancy", 0),
                    "tone": nlp_result.get("sentiment_label", "neutral"),
                },
            }
        ],
    }


def _make_handler(model):
    def _handle_message(ch, method, properties, body):
        payload = json.loads(body)
        text = payload.get("content", "")
        result = model.predict(text)
        output = _format_output(payload, result)
        ch.basic_publish(
            exchange="",
            routing_key=queue_names.analysis,
            body=json.dumps(output, ensure_ascii=False).encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        latency = ""
        ts = payload.get("timestamp")
        if ts:
            latency = f" latency={time.time() - ts:.2f}s"
        logger.info("NLP: post %s -> cluster %s tone=%s relevancy=%s%s",
                     payload.get("post_id"), output["clusterId"],
                     output["posts"][0]["metrics"]["tone"],
                     output["posts"][0]["metrics"]["relevancy"],
                     latency)
    return _handle_message


def run() -> None:
    client = RabbitClient()
    model = RuBertModel()
    client.declare_queue(queue_names.clustered, dlq_name=queue_names.clustered_dlq)
    client.declare_queue(queue_names.analysis)
    thread = client.consume(queue_names.clustered, _make_handler(model),
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
