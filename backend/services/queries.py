"""
queries.py - 百数云查询配置
==============================
这里定义每个统计指标对应的「接口地址」和「筛选条件」。
如果表单字段 ID、筛选逻辑需要调整，只改这一个文件即可。

字段命名规则：百数云字段统一用 _widget_<ID> 格式
筛选条件格式：
    {"rel": "and", "cond": [{"field": "字段名", "method": "eq/range", "value": [...]}]}
"""

from datetime import datetime
import calendar

from services.config import (
    URL_CUSTOMER_COUNT,
    URL_FOLLOWUP_COUNT,
    URL_RECEPTION_COUNT,
    URL_CLUE_COUNT,
    URL_OPPORTUNITY_COUNT,
)


# ════════════════════════════════════════
#  工具：获取本月起止时间字符串
# ════════════════════════════════════════
def _month_range():
    now = datetime.now()
    first = datetime(now.year, now.month, 1, 0, 0, 0)
    last_day = calendar.monthrange(now.year, now.month)[1]
    last = datetime(now.year, now.month, last_day, 23, 59, 59)
    return first.strftime("%Y-%m-%d %H:%M:%S"), last.strftime("%Y-%m-%d %H:%M:%S")


# ════════════════════════════════════════
#  查询列表
#  每项格式：(结果字段名, 接口URL, filter或None)
#  执行时会依次调用，返回 {结果字段名: count}
# ════════════════════════════════════════
def get_queries():
    s, e = _month_range()

    return [
        # ── 客户 ──────────────────────────────────────────────
        (
            "customer_total",           # 客户总数（无筛选）
            URL_CUSTOMER_COUNT,
            None,
        ),
        (
            "customer_new_month",       # 本月新增客户（按创建时间筛选）
            URL_CUSTOMER_COUNT,
            {"rel": "and", "cond": [
                {"field": "createTime", "method": "range", "value": [s, e]},
            ]},
        ),

        # ── 拜访 & 宴请 ────────────────────────────────────────
        (
            "visit_month",              # 本月客户拜访次数
            URL_FOLLOWUP_COUNT,
            {"rel": "and", "cond": [
                # 跟进时间字段
                {"field": "_widget_1767764415125", "method": "range", "value": [s, e]},
                # 跟进类型 = 客户拜访（如字段/选项不同请修改）
                # {"field": "_widget_1765330422881", "method": "eq", "value": ["客户拜访"]},
            ]},
        ),
        (
            "reception_month",          # 本月客户宴请次数
            URL_RECEPTION_COUNT,
            {"rel": "and", "cond": [
                # 宴请时间字段
                {"field": "_widget_1767841428824", "method": "range", "value": [s, e]},
            ]},
        ),

        # ── 线索 ──────────────────────────────────────────────
        (
            "clue_followup_month",      # 本月线索跟进次数
            URL_FOLLOWUP_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1767764415125", "method": "range", "value": [s, e]},
                # 跟进类型 = 线索跟进
                {"field": "_widget_1765330422881", "method": "eq",    "value": ["线索跟进"]},
            ]},
        ),
        (
            "clue_pending",             # 待处理线索（状态=未分配）
            URL_CLUE_COUNT,
            {"rel": "and", "cond": [
                # 线索状态字段
                {"field": "_widget_1767495591693", "method": "eq", "value": ["未分配"]},
            ]},
        ),

        # ── 商机 ──────────────────────────────────────────────
        (
            "opp_followup_month",       # 本月商机跟进次数
            URL_FOLLOWUP_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1767764415125", "method": "range", "value": [s, e]},
                # 跟进类型 = 商机跟进
                {"field": "_widget_1765330422881", "method": "eq",    "value": ["商机跟进"]},
            ]},
        ),
        (
            "opp_total",                # 在跟商机总数（无筛选）
            URL_OPPORTUNITY_COUNT,
            None,
        ),
        (
            "opp_new_month",            # 本月新增商机（按创建时间）
            URL_OPPORTUNITY_COUNT,
            {"rel": "and", "cond": [
                {"field": "createTime", "method": "range", "value": [s, e]},
            ]},
        ),
        (
            "opp_red",                  # 红灯商机
            URL_OPPORTUNITY_COUNT,
            {"rel": "and", "cond": [
                # 商机灯状态字段
                {"field": "_widget_1767768490750", "method": "eq", "value": ["红灯"]},
            ]},
        ),
        (
            "opp_yellow",               # 黄灯商机
            URL_OPPORTUNITY_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1767768490750", "method": "eq", "value": ["黄灯"]},
            ]},
        ),
        (
            "opp_green",                # 绿灯商机
            URL_OPPORTUNITY_COUNT,
            {"rel": "and", "cond": [
                {"field": "_widget_1767768490750", "method": "eq", "value": ["绿灯"]},
            ]},
        ),
    ]
