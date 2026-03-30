"""
销售工作台后端入口
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routes.dashboard import router as dashboard_router
from routes.sales_stats import router as sales_stats_router

app = FastAPI(title="销售工作台 API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard_router)
app.include_router(sales_stats_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8765)
