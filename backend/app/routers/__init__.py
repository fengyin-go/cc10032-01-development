"""业务路由汇总：固定前缀路由先注册，通用资源路由兜底动态路径。"""
from __future__ import annotations

from app.routers import resource, window_metric

# window_metrics 为固定前缀路由，必须排在 /api/{module} 动态路由之前。
ROUTERS = [window_metric, resource]
