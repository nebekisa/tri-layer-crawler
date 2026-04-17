"""
Alert manager for sending notifications.
"""

import requests
import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class AlertManager:
    """Send alerts to various channels."""
    
    def __init__(self, slack_webhook: Optional[str] = None):
        self.slack_webhook = slack_webhook
    
    def send_slack_alert(
        self,
        title: str,
        message: str,
        severity: str = "warning",
        fields: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Send alert to Slack."""
        if not self.slack_webhook:
            logger.warning("No Slack webhook configured")
            return False
        
        color_map = {
            'info': '#36a64f',
            'warning': '#ffcc00',
            'error': '#ff0000',
            'critical': '#8b0000'
        }
        
        payload = {
            "attachments": [{
                "color": color_map.get(severity, '#cccccc'),
                "title": title,
                "text": message,
                "fields": [
                    {"title": k, "value": v, "short": True}
                    for k, v in (fields or {}).items()
                ],
                "footer": "Tri-Layer Intelligence Crawler",
                "ts": datetime.utcnow().timestamp()
            }]
        }
        
        try:
            response = requests.post(self.slack_webhook, json=payload)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False