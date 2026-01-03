# -*- coding: utf-8 -*-
"""
数据获取器
- 从 TimescaleDB 获取 K线数据
- 从 SQLite 获取指标数据
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Any

from src.config import INDICATOR_DB, PROJECT_ROOT

# 数据字段说明
try:
    from src.utils.data_docs import DATA_DOCS
except ImportError:
    DATA_DOCS = {}

# TimescaleDB 连接配置
DB_HOST = os.getenv("TIMESCALE_HOST", "localhost")
DB_PORT = os.getenv("TIMESCALE_PORT", "5433")
DB_USER = os.getenv("TIMESCALE_USER", "postgres")
DB_PASS = os.getenv("TIMESCALE_PASSWORD", "postgres")
DB_NAME = os.getenv("TIMESCALE_DB", "market_data")

DEFAULT_INTERVALS = ["1m", "5m", "15m", "1h", "4h", "1d", "1w"]


def _get_pg_conn():
    """获取 PostgreSQL 连接"""
    import psycopg
    conninfo = f"host={DB_HOST} port={DB_PORT} user={DB_USER} password={DB_PASS} dbname={DB_NAME}"
    return psycopg.connect(conninfo)


def fetch_candles(symbol: str, intervals: List[str] = None) -> Dict[str, List[Dict[str, Any]]]:
    """获取多周期 K线数据"""
    intervals = intervals or DEFAULT_INTERVALS
    candles: Dict[str, List[Dict[str, Any]]] = {}
    
    try:
        conn = _get_pg_conn()
        cur = conn.cursor()
        
        for iv in intervals:
            table = f"market_data.candles_{iv}"
            sql = f"""
                SELECT bucket_ts, open, high, low, close, volume, quote_volume, 
                       trade_count, taker_buy_volume, taker_buy_quote_volume
                FROM {table} 
                WHERE symbol = %s 
                ORDER BY bucket_ts DESC 
                LIMIT 50
            """
            cur.execute(sql, (symbol,))
            rows = cur.fetchall()
            
            parsed = []
            for row in rows:
                parsed.append({
                    "bucket_ts": str(row[0]) if row[0] else None,
                    "open": float(row[1]) if row[1] else None,
                    "high": float(row[2]) if row[2] else None,
                    "low": float(row[3]) if row[3] else None,
                    "close": float(row[4]) if row[4] else None,
                    "volume": float(row[5]) if row[5] else None,
                    "quote_volume": float(row[6]) if row[6] else None,
                    "trade_count": int(row[7]) if row[7] else None,
                    "taker_buy_volume": float(row[8]) if row[8] else None,
                    "taker_buy_quote_volume": float(row[9]) if row[9] else None,
                })
            candles[iv] = parsed
            
        cur.close()
        conn.close()
    except Exception as e:
        # 回退到 psql CLI
        candles = _fetch_candles_psql(symbol, intervals)
    
    return candles


def _fetch_candles_psql(symbol: str, intervals: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """使用 psql CLI 获取 K线（回退方案）"""
    import subprocess
    
    candles: Dict[str, List[Dict[str, Any]]] = {}
    
    for iv in intervals:
        table = f"market_data.candles_{iv}"
        sql = (
            "SELECT bucket_ts,open,high,low,close,volume,quote_volume,trade_count,"
            "taker_buy_volume,taker_buy_quote_volume "
            f"FROM {table} WHERE symbol='{symbol}' ORDER BY bucket_ts DESC LIMIT 50"
        )
        cmd = [
            "psql", "-h", DB_HOST, "-p", DB_PORT, "-U", DB_USER, "-d", DB_NAME,
            "-A", "-F", ",", "-q", "-t", "-P", "footer=off", "-c", sql,
        ]
        env = os.environ.copy()
        env["PGPASSWORD"] = DB_PASS
        
        res = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if res.returncode != 0:
            candles[iv] = []
            continue
            
        parsed = []
        for line in res.stdout.splitlines():
            if not line.strip():
                continue
            parts = line.split(",")
            if len(parts) >= 6:
                parsed.append({
                    "bucket_ts": parts[0],
                    "open": float(parts[1]) if parts[1] else None,
                    "high": float(parts[2]) if parts[2] else None,
                    "low": float(parts[3]) if parts[3] else None,
                    "close": float(parts[4]) if parts[4] else None,
                    "volume": float(parts[5]) if parts[5] else None,
                    "quote_volume": float(parts[6]) if len(parts) > 6 and parts[6] else None,
                    "trade_count": int(parts[7]) if len(parts) > 7 and parts[7] else None,
                    "taker_buy_volume": float(parts[8]) if len(parts) > 8 and parts[8] else None,
                    "taker_buy_quote_volume": float(parts[9]) if len(parts) > 9 and parts[9] else None,
                })
        candles[iv] = parsed
    
    return candles


def fetch_metrics(symbol: str) -> List[Dict[str, Any]]:
    """获取期货指标数据"""
    try:
        conn = _get_pg_conn()
        cur = conn.cursor()
        
        sql = """
            SELECT create_time, symbol, sum_open_interest, sum_open_interest_value,
                   sum_toptrader_long_short_ratio, sum_taker_long_short_vol_ratio
            FROM market_data.binance_futures_metrics_5m
            WHERE symbol = %s
            ORDER BY create_time DESC
            LIMIT 50
        """
        cur.execute(sql, (symbol,))
        rows = cur.fetchall()
        
        result = []
        for row in rows:
            result.append({
                "create_time": str(row[0]) if row[0] else None,
                "symbol": row[1],
                "sum_open_interest": str(row[2]) if row[2] else None,
                "sum_open_interest_value": str(row[3]) if row[3] else None,
                "sum_toptrader_long_short_ratio": str(row[4]) if row[4] else None,
                "sum_taker_long_short_vol_ratio": str(row[5]) if row[5] else None,
            })
        
        cur.close()
        conn.close()
        return result
    except Exception:
        return []


def fetch_indicators(symbol: str) -> Dict[str, Any]:
    """从 SQLite 获取指标数据"""
    db_path = INDICATOR_DB
    indicators: Dict[str, Any] = {}

    if not db_path.exists():
        return {"error": f"数据库不存在: {db_path}"}

    try:
        conn = sqlite3.connect(str(db_path))
    except Exception:
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        except Exception as e:
            return {"error": str(e)}

    cur = conn.cursor()
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    
    for tbl in tables:
        try:
            cols = [d[1] for d in cur.execute(f"PRAGMA table_info('{tbl}')").fetchall()]
            if not cols:
                continue
                
            sym_col = None
            for cand in ["交易对", "symbol", "Symbol", "SYMBOL"]:
                if cand in cols:
                    sym_col = cand
                    break
                    
            if sym_col is None:
                continue
                
            rows = cur.execute(f"SELECT * FROM '{tbl}' WHERE `{sym_col}`=?", (symbol,)).fetchall()
            if rows:
                indicators[tbl] = [dict(zip(cols, r)) for r in rows]
        except Exception as e:
            indicators[tbl] = {"error": str(e)}
            
    cur.close()
    conn.close()
    return indicators


def fetch_payload(symbol: str, interval: str) -> Dict[str, Any]:
    """获取精简数据负载（控制在 100KB 以内）"""
    # 只获取请求的周期和相邻周期
    interval_map = {"1m": ["1m"], "5m": ["5m", "15m"], "15m": ["15m", "1h"], 
                    "1h": ["1h", "4h"], "4h": ["4h", "1d"], "1d": ["1d", "1w"], "1w": ["1w"]}
    intervals = interval_map.get(interval, [interval])
    
    # K线只取最近 20 条
    candles_raw = fetch_candles(symbol, intervals)
    candles = {}
    for iv, data in candles_raw.items():
        candles[iv] = data[:20]  # 只取最近 20 条
    
    # 期货指标只取最近 10 条
    metrics = fetch_metrics(symbol)[:10]
    
    # 指标数据只取关键表
    indicators_raw = fetch_indicators(symbol)
    indicators = {}
    key_tables = ["MACD柱状扫描器.py", "布林带扫描器.py", "KDJ随机指标扫描器.py", 
                  "ATR波幅扫描器.py", "成交量比率扫描器.py", "主动买卖比扫描器.py"]
    for tbl in key_tables:
        if tbl in indicators_raw:
            data = indicators_raw[tbl]
            if isinstance(data, list):
                # 只保留请求周期的数据
                indicators[tbl] = [r for r in data if r.get("周期") == interval][:5]
            else:
                indicators[tbl] = data
    
    return {
        "symbol": symbol,
        "interval": interval,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candles": candles,
        "metrics": metrics,
        "indicators": indicators,
    }


__all__ = ["fetch_payload", "fetch_candles", "fetch_metrics", "fetch_indicators"]
