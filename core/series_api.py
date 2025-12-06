"""Series API client for sending messages."""

import os
import logging
import base64
import requests
from typing import Dict, Any, Optional
from io import BytesIO

logger = logging.getLogger(__name__)


class SeriesAPI:
    """Client for Series iMessage API."""

    def __init__(self, api_key: str, base_url: Optional[str] = None):
        """Initialize Series API client."""
        self.api_key = api_key
        url = base_url or os.getenv('SERIES_API_URL', 'https://series-hackathon-service-202642739529.us-east1.run.app')
        # Remove any comments from URL
        if '#' in url:
            url = url.split('#')[0].strip()
        self.base_url = url.rstrip('/')
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        logger.info(f"Series API initialized: {self.base_url}")

    def send_message(
        self,
        send_from: str,
        chat_id: Optional[int] = None,
        phone_numbers: Optional[list] = None,
        text: str = "",
        attachment: Optional[BytesIO] = None,
        filename: str = "attachment",
        mime_type: str = "application/octet-stream"
    ) -> Optional[Dict[str, Any]]:
        """Send a message via Series API."""
        try:
            if chat_id:
                return self._send_to_chat(chat_id, send_from, text, attachment, filename, mime_type)
            elif phone_numbers:
                return self._create_chat(send_from, phone_numbers, text, attachment, filename, mime_type)
            else:
                logger.error("No chat_id or phone_numbers provided")
                return None
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return None

    def _send_to_chat(
        self,
        chat_id: int,
        send_from: str,
        text: str,
        attachment: Optional[BytesIO],
        filename: str,
        mime_type: str
    ) -> Optional[Dict[str, Any]]:
        """Send to existing chat."""
        url = f"{self.base_url}/api/chats/{chat_id}/chat_messages"

        payload = {
            "send_from": send_from,
            "message": {"text": text}
        }

        if attachment:
            attachment.seek(0)
            data_base64 = base64.b64encode(attachment.read()).decode('utf-8')
            payload["message"]["attachments"] = [{
                "filename": filename,
                "mime_type": mime_type,
                "data_base64": data_base64
            }]

        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=30)
            response.raise_for_status()
            logger.info(f"Sent message to chat {chat_id}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text[:200]}")
            return None

    def _create_chat(
        self,
        send_from: str,
        phone_numbers: list,
        text: str,
        attachment: Optional[BytesIO],
        filename: str,
        mime_type: str
    ) -> Optional[Dict[str, Any]]:
        """Create new chat and send message."""
        url = f"{self.base_url}/api/chats"

        payload = {
            "send_from": send_from,
            "chat": {"phone_numbers": phone_numbers},
            "message": {"text": text}
        }

        if attachment:
            attachment.seek(0)
            data_base64 = base64.b64encode(attachment.read()).decode('utf-8')
            payload["message"]["attachments"] = [{
                "filename": filename,
                "mime_type": mime_type,
                "data_base64": data_base64
            }]

        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=30)
            response.raise_for_status()
            logger.info(f"Created chat with {phone_numbers}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text[:200]}")
            return None

    def get_chat_id(self, message_data: Dict[str, Any]) -> Optional[int]:
        """Extract chat_id from Kafka message."""
        data = message_data.get('data', {}) or {}

        # Try multiple common shapes we have observed from Series events
        chat_id = (
            data.get('chat_id')
            or data.get('id')
            or (data.get('chat') or {}).get('id')
            or (data.get('chat') or {}).get('chat_id')
        )

        if chat_id:
            try:
                return int(chat_id)
            except (TypeError, ValueError):
                logger.warning(f"Non-integer chat_id found in message: {chat_id}")

        logger.warning(f"No chat_id found in message_data: keys={list(data.keys())}")
        return None
