# Legacy V1 — 已废弃的旧版期权采集器

此目录是 deribit-options-data-collector 的 **V1 实现**（2026-05 之前），
已被根目录共享库 + `launch.py` 的 V3.0 架构完全取代。

## 为什么废弃

- V1 为独立 Poetry 包（`src/deribit_options_collector/`），与仓库其余模块零复用
- SQLite + Parquet 双写，运维复杂且 SQLite 在高频写入下是瓶颈
- `launch.py`（V3.0）没有任何一行代码 import `deribit_options_collector`
- 其 `Dockerfile` / `docker-compose.yml` / `config/collector.yaml` 均服务于该旧架构

## 内容

- `src/` 旧采集器源码（REST/WS 客户端、collectors、storage）
- `tests/` 旧测试套件（随包归档，不再运行）
- `Dockerfile` `docker-compose.yml` `entrypoint.sh` `prometheus.yml` 旧部署文件
- `pyproject.toml` Poetry 配置；`run_collector.py` `final_report.py` 一次性脚本
- `config/collector.yaml` 旧配置

## 处置

仅作历史参考。如确认无回溯需求，可整体 `git rm -r legacy_v1/` 并打 tag 存档。
