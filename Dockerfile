FROM python:3.11-slim AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Runtime stage ──
FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /install /usr/local
COPY . .

# Data volume for Parquet files
VOLUME /opt/crypto-data

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV DATA_DIR=/opt/crypto-data
ENV LOG_DIR=/opt/crypto-data/logs

# Non-root user for security
RUN groupadd -r collector && useradd -r -g collector -d /app collector && \
    chown -R collector:collector /app /opt/crypto-data
USER collector

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["python", "deribit-options-data-collector/launch.py", "--mode", "live"]
CMD ["--strategies", "all"]
