# 基于现有生产镜像（nautilus-trader 全依赖环境已就绪，零重建秒级 build）
FROM nautilus_ares-binance:latest

# 仅覆盖采集器为落盘版 + 注入日合并脚本
COPY scripts/production_collector_disk.py /app/scripts/production_collector_disk.py
COPY bin/merge_daily.py /app/merge_daily.py

RUN mkdir -p /app/data/production /app/data/archive

CMD ["python", "scripts/production_collector_disk.py"]
