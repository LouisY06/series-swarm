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
    
    def generate_bucketlist_suggestions(
        self,
        me_items: Dict[str, str],
        partner_items: Dict[str, str]
    ) -> str:
        """
        Given two 5-item bucket lists (travel, skill, food, adventure, creative),
        generate 2–4 concrete activities they could realistically do together.
        """

        def _fmt(items: Dict[str, str]) -> str:
            return (
                f"1) Travel: {items.get('travel', '').strip()}\n"
                f"2) Skill: {items.get('skill', '').strip()}\n"
                f"3) Food: {items.get('food', '').strip()}\n"
                f"4) Adventure: {items.get('adventure', '').strip()}\n"
                f"5) Creative: {items.get('creative', '').strip()}\n"
            )

        me_text = _fmt(me_items)
        partner_text = _fmt(partner_items)

        prompt = f"""
You are helping two people plan activities based on their bucket lists.

Bucket list A:
{me_text}

Bucket list B:
{partner_text}

Your job:
- Find overlaps, complements, or natural combos between their lists.
- Suggest 2–4 specific activities they could realistically do together.
- Each activity should feel like a real plan (location or vibe + what they'd do).
- Focus on shared regions/themes instead of inventing totally new ones.
- Keep it short and friendly.
- No emojis.
- Format as bullet points starting with "- ".

Output only the bullet list, nothing else.
"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a social coordinator who creates concrete, fun joint plans from two bucket lists."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.7,
                max_tokens=250
            )
            suggestions = response.choices[0].message.content.strip()
            return suggestions
        except Exception as e:
            logger.error(f"Error generating bucketlist suggestions: {e}")
            return (
                "- Pick one travel destination you both like and plan a weekend itinerary.\n"
                "- Choose one skill (painting/drawing, music, etc.) and do a mini session together.\n"
                "- Go for a food crawl that fits both your tastes.\n"
                "- Combine your most adventurous idea into a future trip."
            )

    def generate_bucketlist_themes(
        self,
        me_items: Dict[str, str],
        partner_items: Dict[str, str]
    ) -> List[Dict[str, str]]:
        """
        Reduce two bucket lists into 2-4 themes for real-world search.
        Returns a list of dicts with keys: label, query, type.
        """

        def _fmt(items: Dict[str, str]) -> str:
            return (
                f"1) Travel: {items.get('travel', '').strip()}\n"
                f"2) Skill: {items.get('skill', '').strip()}\n"
                f"3) Food: {items.get('food', '').strip()}\n"
                f"4) Adventure: {items.get('adventure', '').strip()}\n"
                f"5) Creative: {items.get('creative', '').strip()}\n"
            )

        me_text = _fmt(me_items)
        partner_text = _fmt(partner_items)

        prompt = f"""
Given these two bucket lists, output a JSON array of 2-4 themes.
Each theme object must have: label, query, type (travel|food|adventure|creative|experience).
Keep queries short (3-6 tokens) for place search.

Bucket list A:
{me_text}

Bucket list B:
{partner_text}

Return ONLY JSON. No extra text.
"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You convert bucket lists into concise search themes for real-world recommendations."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.4,
                max_tokens=300
            )
            content = response.choices[0].message.content.strip()
            import json
            themes = json.loads(content)
            # Basic validation
            cleaned = []
            for t in themes:
                if not isinstance(t, dict):
                    continue
                cleaned.append({
                    "label": t.get("label", "").strip() or "Idea",
                    "query": t.get("query", "").strip(),
                    "type": t.get("type", "").strip() or "experience",
                })
            return cleaned[:4] if cleaned else []
        except Exception as e:
            logger.error(f"Error generating bucketlist themes: {e}")
            # Fallback heuristic themes from the items
            merged = [
                me_items.get("travel", ""),
                partner_items.get("travel", ""),
                me_items.get("food", ""),
                partner_items.get("food", ""),
                me_items.get("adventure", ""),
                partner_items.get("adventure", ""),
                me_items.get("creative", ""),
                partner_items.get("creative", ""),
                me_items.get("skill", ""),
                partner_items.get("skill", ""),
            ]
            merged = [m for m in merged if m and m.strip()]
            top = merged[:3] if merged else ["bucket list"]
            fallback_query = " ".join(top)
            return [
                {"label": "Shared trip", "query": fallback_query, "type": "travel"},
                {"label": "Shared food", "query": " ".join(top[:2]), "type": "food"},
            ]
    
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
        """Ensure profile is updated if needed. Returns (new_level, old_level, changed)."""
        viewer_profile = state_manager.get_profile(viewer_id, other_id)
        old_level = viewer_profile["level"]
        new_level = self.level_for_message_count(total_messages)
        
        # Avoid calling LLM too often
        if (new_level == viewer_profile["level"] and 
            total_messages - viewer_profile["last_message_count"] < 5):
            return new_level, old_level, False
        
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
        return new_level, old_level, new_level != old_level

    def generate_mood(self, conversation: List[Dict[str, str]]) -> str:
        """Summarize tone/mood from recent conversation."""
        conv_text = "\n".join([f"{m['from']}: {m['text']}" for m in conversation[-30:]])
        prompt = f"""Read the recent conversation and describe the current tone in 1-2 sentences using simple words.
Be concise, no emojis.
Conversation:
{conv_text}
"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {'role': 'system', 'content': 'You summarize tone briefly and clearly.'},
                    {'role': 'user', 'content': prompt}
                ],
                temperature=0.4,
                max_tokens=120
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error generating mood: {e}")
            return "The conversation feels neutral and open."

    def generate_topics(self, conversation: List[Dict[str, str]]) -> str:
        """Extract key topics from recent conversation."""
        conv_text = "\n".join([f"{m['from']}: {m['text']}" for m in conversation[-40:]])
        prompt = f"""List 3-6 key topics these two people have discussed, as short bullet points, no emojis.
Conversation:
{conv_text}
"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {'role': 'system', 'content': 'You extract concise topic lists from chat logs.'},
                    {'role': 'user', 'content': prompt}
                ],
                temperature=0.4,
                max_tokens=120
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error generating topics: {e}")
            return "- Small talk\n- Interests\n- Plans"

    def generate_coach_nudge(self, conversation: List[Dict[str, str]]) -> str:
        """Provide a short nudge with summary + suggested question."""
        conv_text = "\n".join([f"{m['from']}: {m['text']}" for m in conversation[-12:]])
        prompt = f"""You are a gentle conversation coach. In 2-3 short lines:
1) Briefly reflect what they seem interested in.
2) Suggest one specific next question they can ask.
No emojis. Keep under 200 characters total.
Conversation:
{conv_text}
"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {'role': 'system', 'content': 'You give concise, friendly conversation nudges.'},
                    {'role': 'user', 'content': prompt}
                ],
                temperature=0.5,
                max_tokens=120
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error generating coach nudge: {e}")
            return "Maybe ask a follow-up about something they mentioned and why it matters to them."

    def generate_rescue_prompt(self, conversation: List[Dict[str, str]]) -> str:
        """Provide a small-talk rescue prompt for stalled conversations."""
        conv_text = "\n".join([f"{m['from']}: {m['text']}" for m in conversation[-12:]])
        prompt = f"""The chat stalled (long gaps or one-word replies).
Give one short, friendly prompt to restart the chat, tailored to what they discussed.
No emojis. Under 150 characters.
Conversation:
{conv_text}
"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {'role': 'system', 'content': 'You revive chats with concise, friendly prompts.'},
                    {'role': 'user', 'content': prompt}
                ],
                temperature=0.6,
                max_tokens=90
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error generating rescue prompt: {e}")
            return "Maybe ask them about something fun they mentioned earlier and why they enjoy it."

