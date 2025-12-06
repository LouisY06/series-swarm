"""Agent D: Audio Briefing Generator (Anchor)."""

import logging
import os
from io import BytesIO
from typing import Dict, Any
from openai import OpenAI

logger = logging.getLogger(__name__)


def generate_audio_briefing(audio_data: Dict[str, Any]) -> BytesIO:
    """
    Generate an audio briefing using OpenAI TTS-1 API.

    Args:
        audio_data: Dictionary containing audio information:
            - text: Text content to convert to speech (required)
            - voice: Voice to use ('alloy', 'echo', 'fable', 'onyx', 'nova', 'shimmer') - default: 'nova'
            - model: TTS model to use - default: 'tts-1'
            - openai_api_key: Optional OpenAI API key (falls back to OPENAI_API_KEY env var)

    Returns:
        BytesIO object containing the audio file (MP3 format)
    """
    try:
        openai_api_key = audio_data.get('openai_api_key') or os.getenv('OPENAI_API_KEY')
        if not openai_api_key:
            raise ValueError("OpenAI API key is required (provide in audio_data or set OPENAI_API_KEY env var)")

        client = OpenAI(api_key=openai_api_key)

        text = audio_data.get('text', '')
        if not text:
            raise ValueError("Text content is required for audio generation")

        voice = audio_data.get('voice', 'nova')
        model = audio_data.get('model', 'tts-1')

        # Generate audio using OpenAI TTS
        response = client.audio.speech.create(
            model=model,
            voice=voice,
            input=text,
        )

        # Convert response to BytesIO
        audio_bytes = response.content
        audio_io = BytesIO(audio_bytes)
        audio_io.seek(0)

        logger.info(f"Generated audio briefing ({len(audio_bytes)} bytes)")
        return audio_io

    except Exception as e:
        logger.error(f"Error generating audio briefing: {e}")
        raise

