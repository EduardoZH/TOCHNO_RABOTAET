# ─────────────────────────────────────────────────────────
# CPU-версия: лёгкий образ, быстрая сборка (~5 мин вместо ~15)
# ─────────────────────────────────────────────────────────
FROM python:3.11-slim

# ── GPU-версия ────────────────────────────────────────────
# Чтобы включить CUDA (NVIDIA L4 и др.):
#   1. Закомментируй строку FROM выше
#   2. Раскомментируй строку ниже:
# FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04
#
#   3. Раскомментируй блок установки Python под Ubuntu:
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     python3.11 python3.11-venv python3-pip \
#     && rm -rf /var/lib/apt/lists/*
# RUN ln -sf /usr/bin/python3.11 /usr/bin/python3 && \
#     ln -sf /usr/bin/python3.11 /usr/bin/python
#
#   4. В requirements.txt замени torch на GPU-версию (см. комментарий там)
#   5. В docker-compose.yml раскомментируй deploy.resources для embedding (см. там)
# ─────────────────────────────────────────────────────────

WORKDIR /app

# Для CPU-only torch нужен отдельный индекс
# GPU-версия: удали строку --extra-index-url и используй обычный pip install
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu \
    torch>=2.2.0+cpu --quiet

COPY requirements.txt .
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu \
    -r requirements.txt

COPY . .

CMD ["python", "main.py"]
