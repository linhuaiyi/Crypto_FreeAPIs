FROM python:3.11-slim as builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 1000 collector && \
    mkdir -p /home/collector/.cache/pypo && \
    chown -R collector:collector /home/collector

WORKDIR /app

COPY --chown=collector:collector pyproject.toml poetry.lock* ./

RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev --no-interaction --no-ansi

COPY --chown=collector:collector src/ ./src/
COPY --chown=collector:collector config/ ./config/

RUN mkdir -p /app/data/raw/option /app/db && \
    chown -R collector:collector /app/data /app/db

USER collector

ENV PYTHONPATH=/app/src:$PYTHONPATH

EXPOSE 8080 9090

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["python", "-m", "deribit_options_collector"]
CMD ["--config", "config/collector.yaml"]
