FROM python:3.11-slim
#FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]