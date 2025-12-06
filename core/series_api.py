"""Series API client for sending messages and attachments."""

import os
import logging
import base64
import requests
from typing import Dict, Any, Optional
from io import BytesIO

logger = logging.getLogger(__name__)


class SeriesAPI:
    """Client for Series iMessage Service API."""

    def __init__(self, api_key: str, base_url: Optional[str] = None):
        """
        Initialize Series API client.

        Args:
            api_key: Series API key
            base_url: Base URL for Series API (defaults to SERIES_API_URL env var or https://api.series.im)
        """
        self.api_key = api_key
        if base_url is None:
            base_url = os.getenv('SERIES_API_URL', os.getenv('API_BASE', 'https://api.series.im'))
        self.base_url = base_url.rstrip('/')
        # Use Authorization header with Bearer token (as per API docs)
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        logger.info(f"Series API client initialized with base URL: {self.base_url}")

    def send_message_with_attachment(
        self,
        send_from: str,
        chat_id: Optional[int] = None,
        phone_numbers: Optional[list] = None,
        text: str = "",
        attachment: Optional[BytesIO] = None,
        filename: str = "attachment",
        mime_type: str = "application/octet-stream"
    ) -> Optional[Dict[str, Any]]:
        """
        Send a message with optional attachment via Series API.

        Args:
            send_from: Phone number to send from (E.164 format)
            chat_id: Existing chat ID (if sending to existing chat)
            phone_numbers: List of recipient phone numbers (E.164 format) - required if no chat_id
            text: Message text
            attachment: BytesIO object containing attachment data
            filename: Attachment filename
            mime_type: Attachment MIME type

        Returns:
            Response data from API or None if failed
        """
        try:
            if chat_id:
                # Send to existing chat
                return self._send_to_existing_chat(chat_id, send_from, text, attachment, filename, mime_type)
            else:
                # Create new chat and send message
                if not phone_numbers:
                    raise ValueError("phone_numbers required when chat_id is not provided")
                return self._create_chat_and_send(send_from, phone_numbers, text, attachment, filename, mime_type)

        except Exception as e:
            logger.error(f"Error sending message via Series API: {e}")
            return None

    def _create_chat_and_send(
        self,
        send_from: str,
        phone_numbers: list,
        text: str,
        attachment: Optional[BytesIO],
        filename: str,
        mime_type: str
    ) -> Optional[Dict[str, Any]]:
        """Create a new chat and send message with attachment."""
        url = f"{self.base_url}/api/chats"

        payload = {
            "send_from": send_from,
            "chat": {
                "phone_numbers": phone_numbers
            },
            "message": {
                "text": text
            }
        }

        # Add attachment if provided
        if attachment:
            attachment.seek(0)
            data_base64 = base64.b64encode(attachment.getvalue()).decode('utf-8')
            payload["message"]["attachments"] = [{
                "filename": filename,
                "mime_type": mime_type,
                "data_base64": data_base64
            }]

        try:
            logger.debug(f"Sending to: {url}")
            logger.debug(f"Payload keys: {list(payload.keys())}")
            response = requests.post(url, json=payload, headers=self.headers, timeout=30)
            response.raise_for_status()
            result = response.json()
            logger.info(f"Message sent successfully to chat {result.get('id', 'unknown')}")
            return result
        except requests.exceptions.Timeout:
            logger.error(f"API request timed out: {url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text[:200]}")
            return None

    def _send_to_existing_chat(
        self,
        chat_id: int,
        send_from: str,
        text: str,
        attachment: Optional[BytesIO],
        filename: str,
        mime_type: str
    ) -> Optional[Dict[str, Any]]:
        """Send message to existing chat."""
        url = f"{self.base_url}/api/chats/{chat_id}/chat_messages"
        logger.info(f"Sending to existing chat {chat_id} from {send_from}")

        payload = {
            "message": {
                "text": text
            }
        }

        # Add attachment if provided
        if attachment:
            attachment.seek(0)
            data_base64 = base64.b64encode(attachment.getvalue()).decode('utf-8')
            payload["message"]["attachments"] = [{
                "filename": filename,
                "mime_type": mime_type,
                "data_base64": data_base64
            }]

        try:
            logger.info(f"POST {url}")
            logger.debug(f"Headers: {dict(self.headers)}")
            response = requests.post(url, json=payload, headers=self.headers, timeout=30)
            response.raise_for_status()
            result = response.json()
            logger.info(f"Message sent successfully to chat {chat_id}")
            return result
        except requests.exceptions.HTTPError as e:
            logger.error(f"API HTTP error: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text[:500]}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text[:500]}")
            return None

    def get_chat_id_from_message(self, message_data: Dict[str, Any]) -> Optional[int]:
        """
        Extract chat_id from incoming message data.

        Args:
            message_data: Message data from Kafka (Series API format)

        Returns:
            Chat ID if found, None otherwise
        """
        data = message_data.get('data', {})
        chat_id = data.get('chat_id')
        if chat_id:
            try:
                # Handle both string and int chat_id
                return int(str(chat_id))
            except (ValueError, TypeError) as e:
                logger.warning(f"Could not convert chat_id to int: {chat_id}, error: {e}")
                pass
        return None

    def get_recipient_phone(self, message_data: Dict[str, Any]) -> Optional[str]:
        """
        Extract recipient phone number from incoming message.

        Args:
            message_data: Message data from Kafka (Series API format)

        Returns:
            Recipient phone number (the sender of the original message)
        """
        data = message_data.get('data', {})
        return data.get('from_phone')

