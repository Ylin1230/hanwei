"""
dashboard.py - 工作台数据查询
从 queries.py 读取查询配置，统一调用百数云 data_count 接口
使用线程池并发请求，大幅缩短总耗时
"""
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Optional

import httpx

from services.config import OFFICE_API_TOKEN, OFFICE_API_TIMEOUT
from services.queries import get_queries

logger = logging.getLogger(__name__)

# 并发线程数（不宜过大，避免触发百数云限流）
_MAX_WORKERS = 5


# ── 百数云请求底层（同步，参考 jinshengda 风格）──────────────────
def _post_office_api(url: str, payload: Dict) -> Dict:
    if not OFFICE_API_TOKEN:
        raise ValueError("OFFICE_API_TOKEN 未配置，请在 .env 中设置")
    token = OFFICE_API_TOKEN
    if not token.lower().startswith("bearer "):
        token = f"Bearer {token}"
    headers = {
        "Authorization": token,
        "Content-Type": "application/json;charset=utf-8",
    }
    with httpx.Client(timeout=OFFICE_API_TIMEOUT) as client:
        resp = client.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()


# ── 单个指标查询（带重试）────────────────────────────────────────
def _fetch_count(url: str, filter_obj: Optional[Dict] = None, retries: int = 2) -> int:
    payload = {"filter": filter_obj} if filter_obj else {}
    for attempt in range(retries + 1):
        try:
            data = _post_office_api(url, payload)
            return data.get("count", 0)
        except Exception as exc:
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))  # 递增等待
            else:
                logger.error("fetch_count url=%s error: %s", url, exc)
                return -1


# ── 汇总所有指标（线程池并发）───────────────────────────────────
def get_dashboard_data() -> Dict:
    queries = list(get_queries())
    result: Dict = {}

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        # 提交所有任务
        future_to_key = {
            executor.submit(_fetch_count, url, filter_obj): key
            for key, url, filter_obj in queries
        }
        # 收集结果
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                result[key] = future.result()
            except Exception as exc:
                logger.error("dashboard key=%s unexpected error: %s", key, exc)
                result[key] = -1

    result["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return result
