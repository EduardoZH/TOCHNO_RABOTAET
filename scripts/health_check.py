"""Health check script — probes all infrastructure components."""

import json
import sys
import time
import urllib.request

RABBIT_API = "http://localhost:15672/api"
RABBIT_CREDS = "guest:guest"
QDRANT_URL = "http://localhost:6333"
REDIS_URL = "redis://localhost:6379"


def check_rabbitmq() -> dict:
    try:
        import base64
        auth = base64.b64encode(RABBIT_CREDS.encode()).decode()
        req = urllib.request.Request(f"{RABBIT_API}/queues",
                                     headers={"Authorization": f"Basic {auth}"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            queues = json.loads(resp.read())
        queue_info = {}
        for q in queues:
            name = q.get("name", "")
            msgs = q.get("messages", 0)
            queue_info[name] = msgs
            if msgs > 1000:
                print(f"  WARNING: Queue '{name}' has {msgs} messages (backpressure!)")
        return {"status": "healthy", "queues": queue_info}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def check_redis() -> dict:
    try:
        import redis
        r = redis.from_url(REDIS_URL, socket_timeout=3)
        r.ping()
        info = r.info("memory")
        mem_mb = info.get("used_memory", 0) / 1024 / 1024
        r.close()
        return {"status": "healthy", "memory_mb": round(mem_mb, 1)}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def check_qdrant() -> dict:
    try:
        req = urllib.request.Request(f"{QDRANT_URL}/collections")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        collections = data.get("result", {}).get("collections", [])
        col_info = {}
        for c in collections:
            name = c.get("name", "")
            try:
                req2 = urllib.request.Request(f"{QDRANT_URL}/collections/{name}")
                with urllib.request.urlopen(req2, timeout=5) as resp2:
                    cdata = json.loads(resp2.read())
                points = cdata.get("result", {}).get("points_count", 0)
                col_info[name] = points
            except Exception:
                col_info[name] = "error"
        return {"status": "healthy", "collections": col_info}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def check_embedding_model() -> dict:
    try:
        t0 = time.time()
        from shared.embeddings.embedder import text_to_embedding
        emb = text_to_embedding("тестовая строка для проверки модели")
        latency = time.time() - t0
        return {"status": "healthy", "dim": len(emb), "latency_ms": round(latency * 1000)}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def main():
    print("=" * 60)
    print("  INFRASTRUCTURE HEALTH CHECK")
    print("=" * 60)

    checks = {
        "RabbitMQ": check_rabbitmq,
        "Redis": check_redis,
        "Qdrant": check_qdrant,
    }

    overall = "healthy"
    for name, func in checks.items():
        result = func()
        status = result.get("status", "unknown")
        icon = "OK" if status == "healthy" else "FAIL"
        print(f"\n  [{icon}] {name}: {status}")
        for k, v in result.items():
            if k != "status":
                print(f"       {k}: {v}")
        if status != "healthy":
            overall = "degraded"

    print(f"\n{'=' * 60}")
    print(f"  Overall: {overall}")
    print(f"{'=' * 60}")
    return 0 if overall == "healthy" else 1


if __name__ == "__main__":
    sys.exit(main())
