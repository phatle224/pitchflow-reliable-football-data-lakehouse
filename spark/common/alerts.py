"""Best-effort alerting for Airflow callbacks and successful warning-quality runs."""

from __future__ import annotations

import json
import logging
import os
from typing import Mapping
from urllib.request import Request, urlopen


def send_webhook_alert(
    *,
    severity: str,
    title: str,
    message: str,
    environment: Mapping[str, str] | None = None,
) -> bool:
    """Post an interoperable Slack/Discord-style webhook payload when configured.

    Alert delivery must not mask a task's original failure. Returning ``False``
    means delivery was intentionally disabled or failed after being logged.
    """

    values = environment if environment is not None else os.environ
    webhook_url = values.get("PITCHFLOW_ALERT_WEBHOOK_URL", "").strip()
    rendered = f"[{severity}] {title}: {message}"
    if not webhook_url:
        logging.info("Alert webhook is not configured; alert not delivered: %s", rendered)
        return False

    payload = json.dumps({"text": rendered, "content": rendered}).encode("utf-8")
    request = Request(webhook_url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(request, timeout=5) as response:  # noqa: S310 - URL is an explicit operator setting.
            response.read()
    except Exception:  # noqa: BLE001 - alert delivery must never hide the primary task outcome.
        logging.exception("Unable to deliver PitchFlow webhook alert.")
        return False

    logging.info("Delivered PitchFlow %s alert.", severity)
    return True
