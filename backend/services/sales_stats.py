"""
sales_stats.py - 按销售人员维度统计年度数据
=============================================
指标：年度目标（预留）、年度成交金额、成交商机数、在跟商机数、客户数量、客户拜访、客户接待

字段说明（商机表 entry=5cce95109bd26d02432c141b）：
  负责人:       _widget_1765332525552  (user, 用 _id 筛选)
  商机阶段:     _widget_1765852822338  (combo, "签订合同"=成交)
  预估项目金额: _widget_1767516232919  (number)
  赢单时间:     _widget_1767928508418  (datetime, 年度成交时间范围)

字段说明（客户表 entry=503e3fa088717299600a46f5）：
  负责人:       _widget_1766401476271  (user)

字段说明（跟进记录表 entry=5c5f98606d6aba37c3eead64）：
  线索负责人:   _widget_1768809898752  (user, 代表跟进人/销售)
  跟进时间:     _widget_1767764415125  (datetime, 子表内，用于年度范围)

字段说明（接待表 entry=f1aa4b82881f55a84894b06d）：
  所属人:       _widget_1767850746394  (user, 代表接待销售)
  接待时间:     _widget_1767841428824  (datetime)
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional

import httpx

from services.config import OFFICE_API_TOKEN, OFFICE_API_TIMEOUT, _url

logger = logging.getLogger(__name__)

# ════════════════════════════════════════════
#  接口地址（可在 config.py 中调整 entry_id）
# ════════════════════════════════════════════
ENTRY_OPPORTUNITY = "5cce95109bd26d02432c141b"
ENTRY_CUSTOMER    = "503e3fa088717299600a46f5"
ENTRY_FOLLOWUP    = "5c5f98606d6aba37c3eead64"
ENTRY_RECEPTION   = "f1aa4b82881f55a84894b06d"

URL_OPP_DATA   = _url(ENTRY_OPPORTUNITY, "data")        # 用于获取成交金额（需遍历求和）
URL_OPP_COUNT  = _url(ENTRY_OPPORTUNITY, "data_count")
URL_CUST_COUNT = _url(ENTRY_CUSTOMER,    "data_count")
URL_FOLLOW_COUNT = _url(ENTRY_FOLLOWUP,  "data_count")
URL_RECEP_COUNT  = _url(ENTRY_RECEPTION, "data_count")

# ════════════════════════════════════════════
#  ★ 年度目标配置（预留）
#  key = 销售人员 user _id，value = 目标金额（元）
#  待接入目标表接口后，用实际数据替换此字典
# ════════════════════════════════════════════
ANNUAL_TARGET_PLACEHOLDER: Dict[str, float] = {
    # "user_id": 目标金额,
    # 示例: "5f04df18efd93f8807a11f29": 10000000,
}

_MAX_WORKERS = 6


# ════════════════════════════════════════════
#  工具函数
# ════════════════════════════════════════════
def _year_range(year: Optional[int] = None):
    """返回指定年份（默认当年）的起止时间字符串"""
    y = year or datetime.now().year
    return f"{y}-01-01 00:00:00", f"{y}-12-31 23:59:59"


def _get_headers() -> Dict:
    token = OFFICE_API_TOKEN
    if not token.lower().startswith("bearer "):
        token = f"Bearer {token}"
    return {
        "Authorization": token,
        "Content-Type": "application/json;charset=utf-8",
    }


def _post(url: str, payload: Dict, retries: int = 2) -> Dict:
    """底层 POST，带重试"""
    for attempt in range(retries + 1):
        try:
            with httpx.Client(timeout=OFFICE_API_TIMEOUT) as client:
                resp = client.post(url, headers=_get_headers(), json=payload)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
            else:
                raise


def _count(url: str, filter_obj: Optional[Dict] = None) -> int:
    """查询记录数"""
    payload = {"filter": filter_obj} if filter_obj else {}
    try:
        data = _post(url, payload)
        return data.get("count", 0)
    except Exception as exc:
        logger.error("_count url=%s error: %s", url, exc)
        return -1


def _sum_amount(url: str, filter_obj: Optional[Dict], field: str, page_size: int = 100) -> float:
    """
    分页拉取所有记录，对 number 字段求和
    百数云 data 接口每次最多返回 page_size 条，需翻页
    """
    payload: Dict = {
        "fields": [field],
        "limit": page_size,
    }
    if filter_obj:
        payload["filter"] = filter_obj

    total = 0.0
    page = 1
    while True:
        payload["page"] = page
        try:
            data = _post(url, payload)
        except Exception as exc:
            logger.error("_sum_amount page=%d url=%s error: %s", page, url, exc)
            break
        items = data.get("data", [])
        if not items:
            break
        for item in items:
            val = item.get(field)
            if val is not None:
                try:
                    total += float(val)
                except (TypeError, ValueError):
                    pass
        if len(items) < page_size:
            break
        page += 1

    return total


# ════════════════════════════════════════════
#  获取所有销售人员列表
#  从商机表、客户表、拜访表、接待表中取负责人并去重
# ════════════════════════════════════════════
def _get_all_sales_users(year: int) -> Dict[str, str]:
    """
    返回 {user_id: name} 字典
    取商机表全量 + 客户表全量的负责人合并
    """
    users: Dict[str, str] = {}

    def _collect_users(entry_url_data: str, user_field: str):
        page = 1
        while True:
            payload = {"fields": [user_field], "limit": 100, "page": page}
            try:
                data = _post(entry_url_data, payload)
            except Exception:
                break
            items = data.get("data", [])
            for item in items:
                u = item.get(user_field)
                if isinstance(u, dict) and u.get("_id") and u.get("status", -99) != -99:
                    users[u["_id"]] = u.get("name", "")
            if len(items) < 100:
                break
            page += 1

    # 从商机表收集
    _collect_users(_url(ENTRY_OPPORTUNITY, "data"), "_widget_1765332525552")
    # 从客户表收集
    _collect_users(_url(ENTRY_CUSTOMER, "data"), "_widget_1766401476271")

    return users


# ════════════════════════════════════════════
#  单个销售人员的年度统计
# ════════════════════════════════════════════
def _stats_for_user(uid: str, name: str, year: int) -> Dict:
    ys, ye = _year_range(year)

    # 并发发起所有查询
    tasks = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as ex:

        # 1. 年度成交金额（拉全量数据求和）
        tasks["deal_amount"] = ex.submit(
            _sum_amount,
            URL_OPP_DATA,
            {"rel": "and", "cond": [
                {"field": "_widget_1765852822338", "method": "eq",    "value": ["签订合同"]},
                {"field": "_widget_1767928508418", "method": "range", "value": [ys, ye]},
                {"field": "_widget_1765332525552", "method": "eq",    "value": [uid]},
            ]},
            "_widget_1767516232919",
        )

        # 2. 成交商机数
        tasks["deal_count"] = ex.submit(
            _count, URL_OPP_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1765852822338", "method": "eq",    "value": ["签订合同"]},
                {"field": "_widget_1767928508418", "method": "range", "value": [ys, ye]},
                {"field": "_widget_1765332525552", "method": "eq",    "value": [uid]},
            ]},
        )

        # 3. 在跟商机数（阶段不含签订合同）
        tasks["active_opp"] = ex.submit(
            _count, URL_OPP_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1765852822338", "method": "ne",    "value": ["签订合同"]},
                {"field": "_widget_1765332525552", "method": "eq",    "value": [uid]},
            ]},
        )

        # 4. 客户数量（负责人=该销售，不限年度，统计名下所有客户）
        tasks["customer_count"] = ex.submit(
            _count, URL_CUST_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1766401476271", "method": "eq", "value": [uid]},
            ]},
        )

        # 5. 客户拜访（年度内，线索负责人=该销售）
        tasks["visit_count"] = ex.submit(
            _count, URL_FOLLOW_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1767764415125", "method": "range", "value": [ys, ye]},
                {"field": "_widget_1768809898752", "method": "eq",    "value": [uid]},
            ]},
        )

        # 6. 客户接待（年度内，所属人=该销售）
        tasks["reception_count"] = ex.submit(
            _count, URL_RECEP_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1767841428824", "method": "range", "value": [ys, ye]},
                {"field": "_widget_1767850746394", "method": "eq",    "value": [uid]},
            ]},
        )

        results = {k: f.result() for k, f in tasks.items()}

    return {
        "user_id":        uid,
        "name":           name,
        # ── 年度目标（预留，待接入目标表接口后替换）──
        "annual_target":  ANNUAL_TARGET_PLACEHOLDER.get(uid, None),
        # ── 实时统计 ──
        "deal_amount":    results["deal_amount"],
        "deal_count":     results["deal_count"],
        "active_opp":     results["active_opp"],
        "customer_count": results["customer_count"],
        "visit_count":    results["visit_count"],
        "reception_count":results["reception_count"],
    }


# ════════════════════════════════════════════
#  主入口
# ════════════════════════════════════════════
def get_sales_stats(year: Optional[int] = None) -> Dict:
    """
    返回所有销售人员的年度统计数据
    {
        "year": 2026,
        "updated_at": "...",
        "list": [ {user_id, name, annual_target, deal_amount, deal_count, ...}, ... ]
    }
    """
    y = year or datetime.now().year
    users = _get_all_sales_users(y)

    if not users:
        return {"year": y, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "list": []}

    # 并发统计每个销售
    stats_list: List[Dict] = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(_stats_for_user, uid, name, y): uid for uid, name in users.items()}
        for future in as_completed(futures):
            try:
                stats_list.append(future.result())
            except Exception as exc:
                logger.error("stats_for_user uid=%s error: %s", futures[future], exc)

    # 按成交金额降序排列
    stats_list.sort(key=lambda x: x.get("deal_amount", 0), reverse=True)

    return {
        "year": y,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "list": stats_list,
    }
