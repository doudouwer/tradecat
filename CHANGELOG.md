# 更新日志

所有重要变更都会记录在此文件中。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)。

## [1.0.0] - 2026-01-04

### 🎉 首个正式版本

#### 新增
- **data-service** - 加密货币 K线/期货数据采集 (WebSocket + REST)
- **markets-service** - 全市场数据采集 (美股/A股/宏观/衍生品定价)
- **trading-service** - 38个技术指标计算引擎
- **telegram-service** - Telegram Bot 交互 (20+ 排行榜卡片)
- **ai-service** - AI 分析 (Wyckoff 方法论, 多模型支持)
- **order-service** - 交易执行 (Avellaneda-Stoikov 做市)

#### 数据规模
- K线数据: 3.73亿条 (2018-至今)
- 期货数据: 9457万条 (2021-至今)

#### 技术栈
- Python 3.10+
- TimescaleDB (PostgreSQL 16)
- TA-Lib, pandas, numpy
- CCXT, Cryptofeed
- python-telegram-bot

---

[1.0.0]: https://github.com/tukuaiai/tradecat/releases/tag/v1.0.0
