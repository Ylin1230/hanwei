"""
月度柱状图统计 API 路由
"""
from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool

from services.monthly_stats import get_monthly_stats

router = APIRouter()


@router.get("/api/monthly_stats", summary="按销售人员维度查询本月统计数据（柱状图用）")
async def get_monthly_stats_api():
    """
    返回格式：
    {
        "month": "2026-03",
        "updated_at": "2026-03-31 09:30:00",
        "list": [
            {
                "user_id": "xxx",
                "name": "张三",
                "deal_amount": 500000.0,       // 本月成交合同额（元）
                "reception_count": 3,          // 本月宴请客户次数
                "customer_new": 2,             // 本月新增客户数量
                "visit_count": 5,              // 本月客户拜访次数
                "opp_won": 1,                  // 本月商机赢单数量
                "opp_new": 3,                  // 本月新增商机数量
                "opp_followup": 8             // 本月商机跟进次数
            },
            ...
        ]
    }
    """
    data = await run_in_threadpool(get_monthly_stats)
    return data
