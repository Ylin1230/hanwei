"""
销售年度统计 API 路由
"""
from fastapi import APIRouter, Query
from fastapi.concurrency import run_in_threadpool
from typing import Optional

from services.sales_stats import get_sales_stats

router = APIRouter()


@router.get("/api/sales_stats", summary="按销售人员维度查询年度统计数据")
async def get_sales_stats_api(year: Optional[int] = Query(None, description="年份，默认当年")):
    """
    返回格式：
    {
        "year": 2026,
        "updated_at": "2026-03-30 20:00:00",
        "list": [
            {
                "user_id": "xxx",
                "name": "张三",
                "annual_target": null,       // 年度目标（预留，暂为 null）
                "deal_amount": 1200000.0,    // 年度成交金额（元）
                "deal_count": 3,             // 成交商机数
                "active_opp": 5,             // 在跟商机数
                "customer_count": 7,         // 名下客户数
                "visit_count": 10,           // 年度客户拜访次数
                "reception_count": 8         // 年度客户接待次数
            },
            ...
        ]
    }
    """
    data = await run_in_threadpool(get_sales_stats, year)
    return data
