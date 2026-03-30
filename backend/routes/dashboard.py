"""
工作台 API 路由
"""
from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool

from services.dashboard import get_dashboard_data

router = APIRouter()


@router.get("/api/dashboard", summary="获取销售工作台统计数据")
async def get_dashboard():
    """
    返回所有统计指标，字段由 services/queries.py 的 get_queries() 决定。
    新增/删除/修改指标只需改 queries.py，路由无需变动。
    """
    try:
        # dashboard.py 是同步阻塞调用，放到线程池避免阻塞事件循环
        data = await run_in_threadpool(get_dashboard_data)
        return data
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
