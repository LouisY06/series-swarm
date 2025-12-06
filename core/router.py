"""LLM-based router for intent classification and agent dispatch."""

import logging
from typing import Callable, Dict, Any, Optional
from openai import OpenAI

from agents import (
    generate_ticket,
    generate_calendar_invite,
    generate_vcard,
    generate_audio_briefing,
)

logger = logging.getLogger(__name__)


class Router:
    """Router that classifies intents using LLM and dispatches to appropriate agents."""

    # Intent to agent mapping
    AGENT_MAP = {
        'ticket': generate_ticket,
        'calendar': generate_calendar_invite,
        'contact': generate_vcard,
        'audio': generate_audio_briefing,
    }

    def __init__(self, openai_api_key: str, model: str = 'gpt-4o-mini'):
        """
        Initialize the router with OpenAI client.

        Args:
            openai_api_key: OpenAI API key
            model: Model to use for classification (default: gpt-4o-mini)
        """
        self.client = OpenAI(api_key=openai_api_key)
        self.model = model
        logger.info(f"Router initialized with model: {model}")

    def classify_intent(self, message: Dict[str, Any]) -> Optional[str]:
        """
        Classify the intent of an incoming message using LLM.

        Args:
            message: Incoming message dictionary

        Returns:
            Intent string ('ticket', 'calendar', 'contact', 'audio') or None if classification fails
        """
        # Extract text content from message
        text = message.get('text', '') or message.get('content', '') or str(message)

        prompt = f"""You are an intent classifier for SeriesSwarm, an event-driven system that generates mobile assets.

Classify the following message into one of these intents:
- "ticket": User wants a PDF ticket for an event
- "calendar": User wants a calendar invite/event
- "contact": User wants a vCard contact file
- "audio": User wants an audio briefing or voice message

Message: {text}

Respond with ONLY the intent keyword (ticket, calendar, contact, or audio)."""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {'role': 'system', 'content': 'You are a precise intent classifier. Respond with only the intent keyword.'},
                    {'role': 'user', 'content': prompt}
                ],
                temperature=0.1,
                max_tokens=10,
            )

            intent = response.choices[0].message.content.strip().lower()

            # Validate intent
            if intent in self.AGENT_MAP:
                logger.info(f"Classified intent: {intent}")
                return intent
            else:
                logger.warning(f"Invalid intent classified: {intent}, defaulting to None")
                return None

        except Exception as e:
            logger.error(f"Error classifying intent: {e}")
            return None

    def route(self, message: Dict[str, Any]) -> Optional[Callable]:
        """
        Route a message to the appropriate agent function.

        Args:
            message: Incoming message dictionary

        Returns:
            Agent function if intent is classified, None otherwise
        """
        intent = self.classify_intent(message)

        if intent is None:
            logger.warning("Could not classify intent, no agent dispatched")
            return None

        agent_func = self.AGENT_MAP.get(intent)
        if agent_func is None:
            logger.error(f"No agent function found for intent: {intent}")
            return None

        logger.info(f"Routing to agent for intent: {intent}")
        return agent_func

