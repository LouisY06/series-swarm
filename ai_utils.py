"""LLM utilities for preview generation and profile building."""

import os
import logging
from typing import List, Dict, Any, Optional
from openai import OpenAI

logger = logging.getLogger(__name__)


class AIUtils:
    """Utilities for AI-powered features."""
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize OpenAI client."""
        api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY required")
        
        self.client = OpenAI(api_key=api_key)
        self.model = "gpt-4o-mini"
    
    def generate_preview(self, this_intro: str, other_intro: str) -> str:
        """Generate a preview of the other person from their intro."""
        prompt = f"""You help introduce two anonymous people via text.

Person A intro:
{this_intro}

Person B intro:
{other_intro}

Write 2 or 3 short bullet points describing Person B from A's point of view.
Focus on what they might find interesting or relevant.
Use under 300 characters total.
Do not invent facts. Do not use emojis.
Format as simple bullet points, one per line starting with "-".
"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        'role': 'system',
                        'content': 'You are a helpful assistant that creates brief, accurate introductions between people.'
                    },
                    {'role': 'user', 'content': prompt}
                ],
                temperature=0.7,
                max_tokens=150
            )
            
            preview = response.choices[0].message.content.strip()
            logger.info(f"Generated preview: {preview[:100]}...")
            return preview
            
        except Exception as e:
            logger.error(f"Error generating preview: {e}")
            # Fallback
            return f"- Based on their intro, they seem interesting.\n- They're here to meet people.\n- Start a conversation to learn more!"

    def generate_onboarding_guidance(self) -> str:
        """Generate a short, actionable prompt to guide a new user on what to send."""
        prompt = """You are helping someone join an anonymous chat matchmaking service.
Write 3-4 short bullet points telling them what to send as their intro.
Keep it under 400 characters total. Be specific and actionable.
Do not use emojis."""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        'role': 'system',
                        'content': 'You are concise, friendly, and give clear instructions.'
                    },
                    {'role': 'user', 'content': prompt}
                ],
                temperature=0.5,
                max_tokens=180
            )
            guidance = response.choices[0].message.content.strip()
            logger.info(f"Generated onboarding guidance: {guidance[:100]}...")
            return guidance
        except Exception as e:
            logger.error(f"Error generating onboarding guidance: {e}")
            return (
                "- Share who you are (e.g., student, role, interests)\n"
                "- Say what you’re looking for here\n"
                "- Add 1-2 topics you enjoy discussing\n"
                "- Mention any boundaries (e.g., keep it casual)"
            )
    
    def generate_profile_resume(
        self,
        viewer_id: str,
        other_id: str,
        conversation: List[Dict[str, str]],
        level: int,
        previous_resume: str = ""
    ) -> str:
        """Generate or update a profile resume based on conversation."""
        
        # Build conversation context
        conv_text = "\n".join([
            f"{msg['from']}: {msg['text']}"
            for msg in conversation[-50:]  # Last 50 messages for context
        ])
        
        level_descriptions = {
            0: "very basic observations",
            1: "general characteristics, interests, and communication style",
            2: "deeper insights about motivations, recurring themes, and personality",
            3: "rich description including shared jokes, topics you return to, and how your personalities complement each other"
        }
        
        level_desc = level_descriptions.get(level, level_descriptions[1])
        
        prompt = f"""You are building a personalized profile of someone based only on their conversation with another person.

Conversation history:
{conv_text}

Previous profile (if any):
{previous_resume if previous_resume else "None - this is the first profile"}

Current level: {level}
At this level, focus on: {level_desc}

Write a profile resume (2-5 bullet points) describing what you've learned about the person they're talking to.
- Be specific and reference actual things from the conversation
- Use natural, friendly language
- Focus on what makes them interesting or unique
- If level 3, mention shared moments or inside jokes
- Do not use emojis
- Keep it concise (under 400 characters for level 1-2, up to 600 for level 3)

Format as bullet points, one per line starting with "-".
"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        'role': 'system',
                        'content': 'You are a thoughtful observer who builds accurate, personalized profiles of people based on their conversations.'
                    },
                    {'role': 'user', 'content': prompt}
                ],
                temperature=0.7,
                max_tokens=300 if level < 3 else 400
            )
            
            resume = response.choices[0].message.content.strip()
            logger.info(f"Generated profile resume (level {level}): {resume[:100]}...")
            return resume
            
        except Exception as e:
            logger.error(f"Error generating profile resume: {e}")
            # Fallback based on level
            if level == 0:
                return ""
            elif level == 1:
                return "- They seem engaged in the conversation.\n- Keep talking to learn more about them."
            elif level == 2:
                return "- You've had a good conversation so far.\n- I'm learning more about their interests and style."
            else:
                return "- You've had a deep conversation.\n- I've learned a lot about their personality and interests."
    
    def level_for_message_count(self, count: int) -> int:
        """Determine profile level based on message count."""
        if count >= 100:
            return 3
        if count >= 40:
            return 2
        if count >= 10:
            return 1
        return 0
    
    def ensure_profile_updated(
        self,
        state_manager,
        viewer_id: str,
        other_id: str,
        total_messages: int
    ):
        """Ensure profile is updated if needed."""
        viewer_profile = state_manager.get_profile(viewer_id, other_id)
        new_level = self.level_for_message_count(total_messages)
        
        # Avoid calling LLM too often
        if (new_level == viewer_profile["level"] and 
            total_messages - viewer_profile["last_message_count"] < 5):
            return
        
        # Get conversation
        conversation = state_manager.get_conversation(viewer_id, other_id)
        
        # Generate new resume
        new_resume = self.generate_profile_resume(
            viewer_id=viewer_id,
            other_id=other_id,
            conversation=conversation,
            level=new_level,
            previous_resume=viewer_profile["resume"]
        )
        
        # Update profile
        viewer_profile["level"] = new_level
        viewer_profile["resume"] = new_resume
        viewer_profile["last_message_count"] = total_messages
        
        state_manager.set_profile(viewer_id, other_id, viewer_profile)
        logger.info(f"Updated profile for {viewer_id} viewing {other_id}: level {new_level}")

