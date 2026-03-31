"""
monthly_stats.py - 按销售人员维度统计本月数据（柱状图用）
============================================================
指标：
1. 本月成交合同额（销售=负责人，商机阶段=签订合同）
2. 本月宴请客户次数（接待人=销售，接待时间本月）
3. 本月新增客户数量（负责人=销售，创建时间本月）
4. 本月客户拜访次数（跟进人=销售，跟进时间本月，跟进类型=客户拜访）
5. 本月商机赢单数量（销售=负责人，商机阶段=签订合同，赢单时间本月）
6. 本月新增商机数量（销售=负责人，创建时间本月）
7. 本月商机跟进次数（跟进人=销售，跟进时间本月，跟进类型=商机跟进）

字段说明：
- 商机表 (entry=5cce95109bd26d02432c141b):
  负责人: _widget_1765332525552 (user)
  商机阶段: _widget_1765852822338 (combo)
  预估项目金额: _widget_1767516232919 (number)
  赢单时间: _widget_1767928508418 (datetime)

- 客户表 (entry=503e3fa088717299600a46f5):
  负责人: _widget_1766401476271 (user)

- 跟进记录表 (entry=5c5f98606d6aba37c3eead64):
  跟进人: _widget_1768809898752 (user)
  跟进时间: _widget_1767764415125 (datetime)
  跟进类型: _widget_1765330422881 (combo)

- 接待表 (entry=f1aa4b82881f55a84894b06d):
  接待人: _widget_1767850746394 (user)
  接待时间: _widget_1767841428824 (datetime)
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List

import httpx

from services.config import OFFICE_API_TOKEN, OFFICE_API_TIMEOUT, _url

logger = logging.getLogger(__name__)

# 表 entry ID
ENTRY_OPPORTUNITY = "5cce95109bd26d02432c141b"
ENTRY_CUSTOMER    = "503e3fa088717299600a46f5"
ENTRY_FOLLOWUP    = "5c5f98606d6aba37c3eead64"
ENTRY_RECEPTION   = "f1aa4b82881f55a84894b06d"

URL_OPP_DATA   = _url(ENTRY_OPPORTUNITY, "data")
URL_OPP_COUNT  = _url(ENTRY_OPPORTUNITY, "data_count")
URL_CUST_COUNT = _url(ENTRY_CUSTOMER,    "data_count")
URL_FOLLOW_COUNT = _url(ENTRY_FOLLOWUP,  "data_count")
URL_RECEP_COUNT  = _url(ENTRY_RECEPTION, "data_count")

_MAX_WORKERS = 6


# ════════════════════════════════════════════
#  工具函数
# ════════════════════════════════════════════
def _month_range():
    """返回本月起止时间字符串"""
    now = datetime.now()
    first = datetime(now.year, now.month, 1, 0, 0, 0)
    last_day = 31  # 简化处理，百数云 range 会自动处理超出部分
    last = datetime(now.year, now.month, last_day, 23, 59, 59)
    return first.strftime("%Y-%m-%d %H:%M:%S"), last.strftime("%Y-%m-%d %H:%M:%S")


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


def _count(url: str, filter_obj: Dict = None) -> int:
    """查询记录数"""
    payload = {"filter": filter_obj} if filter_obj else {}
    try:
        data = _post(url, payload)
        return data.get("count", 0)
    except Exception as exc:
        logger.error("_count url=%s error: %s", url, exc)
        return -1


def _sum_amount(url: str, filter_obj: Dict, field: str, page_size: int = 100) -> float:
    """分页拉取所有记录，对 number 字段求和"""
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
# ════════════════════════════════════════════
def _get_all_sales_users() -> Dict[str, str]:
    """返回 {user_id: name} 字典"""
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

    # 从商机表和客户表收集
    _collect_users(_url(ENTRY_OPPORTUNITY, "data"), "_widget_1765332525552")
    _collect_users(_url(ENTRY_CUSTOMER, "data"), "_widget_1766401476271")

    return users


# ════════════════════════════════════════════
#  单个销售人员的本月统计
# ════════════════════════════════════════════
def _monthly_stats_for_user(uid: str, name: str, month_start: str, month_end: str) -> Dict:
    """返回单个销售人员本月的7项统计"""

    tasks = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as ex:
        # 1. 本月成交合同额（商机阶段=签订合同 + 赢单时间本月）
        tasks["deal_amount"] = ex.submit(
            _sum_amount,
            URL_OPP_DATA,
            {"rel": "and", "cond": [
                {"field": "_widget_1765852822338", "method": "eq",    "value": ["签订合同"]},
                {"field": "_widget_1767928508418", "method": "range", "value": [month_start, month_end]},
                {"field": "_widget_1765332525552", "method": "eq",    "value": [uid]},
            ]},
            "_widget_1767516232919",
        )

        # 2. 本月宴请客户次数（接待人=销售 + 接待时间本月）
        tasks["reception_count"] = ex.submit(
            _count, URL_RECEP_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1767841428824", "method": "range", "value": [month_start, month_end]},
                {"field": "_widget_1767850746394", "method": "eq",    "value": [uid]},
            ]},
        )

        # 3. 本月新增客户数量（负责人=销售 + 创建时间本月）
        tasks["customer_new"] = ex.submit(
            _count, URL_CUST_COUNT,
            {"rel": "and", "cond": [
                {"field": "createTime", "method": "range", "value": [month_start, month_end]},
                {"field": "_widget_1766401476271", "method": "eq",    "value": [uid]},
            ]},
        )

        # 4. 本月客户拜访次数（跟进人=销售 + 跟进时间本月 + 跟进类型=客户拜访）
        tasks["visit_count"] = ex.submit(
            _count, URL_FOLLOW_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1767764415125", "method": "range", "value": [month_start, month_end]},
                {"field": "_widget_1768809898752", "method": "eq",    "value": [uid]},
                {"field": "_widget_1765330422881", "method": "eq",     "value": ["客户拜访"]},
            ]},
        )

        # 5. 本月商机赢单数量（销售=负责人 + 商机阶段=签订合同 + 赢单时间本月）
        tasks["opp_won"] = ex.submit(
            _count, URL_OPP_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1765332525552", "method": "eq",    "value": [uid]},
                {"field": "_widget_1765852822338", "method": "eq",    "value": ["签订合同"]},
                {"field": "_widget_1767928508418", "method": "range", "value": [month_start, month_end]},
            ]},
        )

        # 6. 本月新增商机数量（销售=负责人 + 创建时间本月）
        tasks["opp_new"] = ex.submit(
            _count, URL_OPP_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1765332525552", "method": "eq",    "value": [uid]},
                {"field": "createTime", "method": "range", "value": [month_start, month_end]},
            ]},
        )

        # 7. 本月商机跟进次数（跟进人=销售 + 跟进时间本月 + 跟进类型=商机跟进）
        tasks["opp_followup"] = ex.submit(
            _count, URL_FOLLOW_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1767764415125", "method": "range", "value": [month_start, month_end]},
                {"field": "_widget_1768809898752", "method": "eq",    "value": [uid]},
                {"field": "_widget_1765330422881", "method": "eq",    "value": ["商机跟进"]},
            ]},
        )

        results = {k: f.result() for k, f in tasks.items()}

    return {
        "user_id": uid,
        "name": name,
        "deal_amount": results["deal_amount"],      # 成交合同额（元）
        "reception_count": results["reception_count"],  # 宴请次数
        "customer_new": results["customer_new"],    # 新增客户
        "visit_count": results["visit_count"],      # 客户拜访次数
        "opp_won": results["opp_won"],              # 商机赢单数
        "opp_new": results["opp_new"],              # 新增商机数
        "opp_followup": results["opp_followup"],    # 商机跟进次数
    }


# ════════════════════════════════════════════
#  主入口
# ════════════════════════════════════════════
def get_monthly_stats() -> Dict:
    """
    返回所有销售人员本月的统计数据（柱状图用）
    """
    month_start, month_end = _month_range()
    users = _get_all_sales_users()

    if not users:
        return {
            "month": datetime.now().strftime("%Y-%m"),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "list": []
        }

    stats_list: List[Dict] = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(_monthly_stats_for_user, uid, name, month_start, month_end): uid
                   for uid, name in users.items()}
        for future in as_completed(futures):
            try:
                stats_list.append(future.result())
            except Exception as exc:
                logger.error("monthly_stats_for_user uid=%s error: %s", futures[future], exc)

    # 按成交金额降序排列
    stats_list.sort(key=lambda x: x.get("deal_amount", 0), reverse=True)

    return {
        "month": datetime.now().strftime("%Y-%m"),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "list": stats_list,
    }
