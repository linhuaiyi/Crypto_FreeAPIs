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
    apt-get install -y --no-install-recommends curl procps && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /install /usr/local
COPY . .

# V3.0 launch.py 从 config_strategy.yaml 读 data_dir="./data" (相对 CWD=/app),
# 数据实际落点 /app/data; volume 必须挂在这里而非自定义路径
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV LOG_DIR=/app/data/logs

# Non-root user for security; VOLUME 不会在 build 期创建目录, 需显式 mkdir
RUN mkdir -p /app/data/logs && \
    groupadd -r collector && useradd -r -g collector -d /app collector && \
    chown -R collector:collector /app
USER collector

VOLUME /app/data

# test 模式 60s 自退出; live 模式无 HTTP 健康端点, 用进程存活检查
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD pgrep -f "deribit-options-data-collector/launch.py" >/dev/null || exit 1

ENTRYPOINT ["python", "deribit-options-data-collector/launch.py", "--mode", "live"]
CMD ["--strategies", "all"]
