"""异常检测领域服务：消费实时计算数据检测异常并触发告警。

数据流: 实时数据 → anomaly_detector（异常检测）→ alert_policy（告警策略）
核心逻辑: 基于滑动窗口的统计异常检测(Z-score)、阈值检测、趋势分析。
"""
from __future__ import annotations

import math
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class AnomalyDetectorService(BaseModuleService):
    MODULE = "anomaly_detector"
    ENTITY = "检测器"
    LIST_FIELDS = ["检测器名称", "监控源", "检测算法", "敏感度", "观察窗口", "基线周期", "触发阈值"]
    REQUIRED_FIELDS = ["检测器名称", "监控源", "检测算法"]
    STATUS_ORDER = ["草稿", "已激活", "抑制中", "已停用"]
    ACTION_RULES = {"激活检测": "草稿", "停用检测": "已激活"}
    NEGATIVE_ACTIONS = ["停用检测"]

    def detect_anomalies(
        self,
        detector_name: str,
        data_points: list[float],
        alert_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """对数据点序列执行异常检测。

        支持三种检测算法:
          - zscore: Z-Score 异常检测，超过3σ标记异常
          - threshold: 绝对阈值检测
          - trend: 趋势变化率检测

        检测到异常时自动写入 alert_policy 生成告警记录。
        """
        # 1. 查找检测器配置
        detector = None
        for r in store.rows(self.MODULE):
            if r.get("检测器名称") == detector_name:
                detector = r
                break
        if detector is None:
            # 使用默认参数
            algorithm = "zscore"
            sensitivity = 3.0
            window_size = 20
        else:
            algorithm = detector.get("检测算法", "zscore")
            try:
                sensitivity = float(detector.get("敏感度", "3.0"))
            except (ValueError, TypeError):
                sensitivity = 3.0
            try:
                window_size = int(detector.get("观察窗口", "20"))
            except (ValueError, TypeError):
                window_size = 20

        anomalies: list[dict[str, Any]] = []

        if algorithm == "zscore":
            anomalies = self._zscore_detect(data_points, sensitivity, window_size)
        elif algorithm == "threshold":
            anomalies = self._threshold_detect(data_points, sensitivity)
        elif algorithm == "trend":
            anomalies = self._trend_detect(data_points, sensitivity)

        # 2. 检测到异常时写入告警
        alert_result = {"triggered": False, "alerts": 0}
        if anomalies and alert_rule_name:
            alert_result = self._create_alert_records(
                detector_name, anomalies, alert_rule_name,
            )

        return {
            "detector": detector_name,
            "algorithm": algorithm,
            "total_points": len(data_points),
            "anomaly_count": len(anomalies),
            "anomaly_rate": len(anomalies) / max(len(data_points), 1),
            "anomalies": anomalies[:10],
            "alert": alert_result,
            "timestamp": time.time(),
        }

    @staticmethod
    def _zscore_detect(
        values: list[float], threshold: float, window: int,
    ) -> list[dict[str, Any]]:
        """Z-Score 滑动窗口异常检测。"""
        anomalies = []
        for i in range(window, len(values)):
            window_vals = values[i - window : i]
            mean = sum(window_vals) / window
            variance = sum((v - mean) ** 2 for v in window_vals) / window
            std_dev = math.sqrt(variance)
            if std_dev > 0:
                z = abs(values[i] - mean) / std_dev
                if z > threshold:
                    anomalies.append({
                        "index": i,
                        "value": values[i],
                        "z_score": round(z, 4),
                        "mean": round(mean, 4),
                        "std_dev": round(std_dev, 4),
                        "type": "zscore_outlier",
                    })
        return anomalies

    @staticmethod
    def _threshold_detect(
        values: list[float], threshold: float,
    ) -> list[dict[str, Any]]:
        """绝对阈值异常检测。"""
        anomalies = []
        for i, v in enumerate(values):
            if v > threshold:
                anomalies.append({
                    "index": i,
                    "value": v,
                    "threshold": threshold,
                    "exceed_by": v - threshold,
                    "type": "threshold_breach",
                })
        return anomalies

    @staticmethod
    def _trend_detect(
        values: list[float], sensitivity: float,
    ) -> list[dict[str, Any]]:
        """趋势变化率检测：相邻窗口变化率超过敏感度即为异常。"""
        anomalies = []
        win = max(5, int(len(values) * 0.1))
        for i in range(win * 2, len(values)):
            prev_avg = sum(values[i - win * 2 : i - win]) / win
            curr_avg = sum(values[i - win : i]) / win
            if prev_avg > 0:
                change_rate = abs(curr_avg - prev_avg) / prev_avg
                if change_rate > sensitivity:
                    anomalies.append({
                        "index": i,
                        "value": values[i],
                        "prev_avg": round(prev_avg, 4),
                        "curr_avg": round(curr_avg, 4),
                        "change_rate": round(change_rate, 4),
                        "type": "trend_change",
                    })
        return anomalies

    def _create_alert_records(
        self,
        detector_name: str,
        anomalies: list[dict[str, Any]],
        alert_rule_name: str,
    ) -> dict[str, Any]:
        """将检测到的异常写入告警策略模块。"""
        alert_row = None
        for r in store.rows("alert_policy"):
            if r.get("规则名称") == alert_rule_name:
                alert_row = r
                break

        for anomaly in anomalies:
            alert_entry = {
                "id": max(
                    (int(r.get("id", 0)) for r in store.rows("alert_policy")), default=0
                ) + 1,
                "规则名称": alert_rule_name,
                "监控指标": f"{detector_name}_{anomaly['type']}",
                "触发条件": f"value={anomaly.get('value', 'N/A')}",
                "告警级别": "CRITICAL" if anomaly.get("type") == "threshold_breach" else "WARNING",
                "沉默窗口": "300s",
                "通知方式": "push",
                "升级路径": "oncall",
                "status": "已启用",
                "pending": True,
                "abnormal": True,
                "_detector": detector_name,
                "_anomaly_detail": anomaly,
                "_created_at": time.time(),
            }
            store.rows("alert_policy").append(alert_entry)

        return {
            "triggered": len(anomalies) > 0,
            "alerts": len(anomalies),
            "alert_rule": alert_rule_name,
            "level": alert_row.get("告警级别", "INFO") if alert_row else "INFO",
        }