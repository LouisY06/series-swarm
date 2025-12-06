"""SeriesSwarm - Anonymous Matchmaking Switchboard."""

import os
import sys
import logging
import signal
import threading
import time
import uuid
import urllib.parse
from typing import Optional, Dict, Any
from dotenv import load_dotenv

from core import KafkaConsumer, KafkaProducer, SeriesAPI, Switchboard, CommandHandler
from core.commands import normalize_name_input
from agents import generate_vcard
from state import StateManager, Status
from ai_utils import AIUtils
from collections import defaultdict
from icebreakers.imessage import parse_statements, format_partner_share
from icebreakers.bucketlist import parse_bucketlist, format_shared_bucketlist, format_captured
from realworld.bucketlist import get_real_world_recommendations, get_top_bucketlist_spots, get_top_spots_for_themes

# Music Quiz feature (optional - gracefully handles missing dependencies)
try:
    from features.music_quiz import SpotifyClient, GeniusClient, QuizGame, OAuthCallbackServer
    MUSIC_QUIZ_AVAILABLE = True
except ImportError:
    MUSIC_QUIZ_AVAILABLE = False

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SeriesSwarm:
    """Main application class."""

    def __init__(self):
        """Initialize components."""
        # Load config
        self.kafka_broker = os.getenv('KAFKA_BROKER')
        self.kafka_topic = os.getenv('KAFKA_TOPIC_IN', 'hackathon-inbound')
        self.kafka_group_id = os.getenv('KAFKA_GROUP_ID', 'series-swarm-group')
        self.kafka_client_id = os.getenv('KAFKA_CLIENT_ID')
        self.kafka_sasl_username = os.getenv('KAFKA_SASL_USERNAME')
        self.kafka_sasl_password = os.getenv('KAFKA_SASL_PASSWORD')
        self.series_api_key = os.getenv('SERIES_API_KEY')
        self.sender_number = os.getenv('SENDER_NUMBER')

        # Validate
        if not self.kafka_broker:
            raise ValueError("KAFKA_BROKER required")
        if not self.series_api_key:
            raise ValueError("SERIES_API_KEY required")
        if not self.sender_number:
            raise ValueError("SENDER_NUMBER required")

        # Initialize components
        logger.info("Initializing SeriesSwarm...")
        
        self.consumer = KafkaConsumer(
            self.kafka_broker,
            self.kafka_topic,
            group_id=self.kafka_group_id,
            sasl_username=self.kafka_sasl_username,
            sasl_password=self.kafka_sasl_password,
            client_id=self.kafka_client_id
        )
        
        self.switchboard = Switchboard()
        self.commands = CommandHandler()
        self.api = SeriesAPI(self.series_api_key)
        self.state = StateManager()
        self.ai = AIUtils()
        # Track users who just received an AI suggestion; skip AI replies on their immediate response.
        self.skip_ai_after_prompt = set()
        self.running = False
        self.processed_messages = set()  # Track processed messages to prevent duplicates
        
        # Initialize music quiz feature if available
        self.quiz_game = None
        self.oauth_server = None
        if MUSIC_QUIZ_AVAILABLE:
            try:
                spotify_client = SpotifyClient()
                genius_client = GeniusClient()
                if spotify_client.is_available() and genius_client.is_available():
                    self.quiz_game = QuizGame(spotify_client, genius_client)
                    
                    # Start OAuth callback server
                    def on_spotify_auth(user_phone, success):
                        if success and user_phone:
                            chat_id = self.state.get_chat_id(user_phone) or self.switchboard.get_chat_id(user_phone)
                            if chat_id:
                                self.send(chat_id, user_phone, "Spotify connected! Use /quiz to start a music game with your partner.")
                    
                    self.oauth_server = OAuthCallbackServer(
                        spotify_client,
                        on_auth_complete=on_spotify_auth
                    )
                    self.oauth_server.start(threaded=True)
                    logger.info("Music quiz feature initialized with OAuth server")
                else:
                    logger.warning("Music quiz feature disabled (missing API keys)")
            except Exception as e:
                logger.warning(f"Music quiz feature disabled: {e}")

        logger.info("SeriesSwarm initialized")

    def send(self, chat_id: Optional[int], phone: str, text: str, 
             attachment=None, filename="", mime_type="") -> bool:
        """Send a message. Always prefer chat_id to avoid creating new chats."""
        # Try to get chat_id from stored state if not provided
        if not chat_id:
            chat_id = self.state.get_chat_id(phone) or self.switchboard.get_chat_id(phone)
            if chat_id:
                logger.debug(f"Retrieved stored chat_id {chat_id} for {phone}")
        
        # CRITICAL: Never create new chats - only send if we have a chat_id
        # Creating new chats triggers Series API to send contact cards automatically
        if not chat_id:
            logger.error(f"Cannot send message to {phone}: no chat_id available. Message: {text[:50]}")
            return False
        
        logger.info(f"Sending message to {phone} (chat_id: {chat_id}): {text[:50]}")
        result = self.api.send_message(
            send_from=self.sender_number,
            chat_id=chat_id,
            phone_numbers=None,  # Never create new chats - always use chat_id
            text=text,
            attachment=attachment,
            filename=filename,
            mime_type=mime_type
        )
        if result:
            logger.info(f"Successfully sent message to {phone}")
        else:
            logger.error(f"Failed to send message to {phone}")
        return result is not None

    def process_message(self, message: Dict[str, Any]) -> bool:
        """Process an incoming message."""
        try:
            # Deduplication: check if we've already processed this message
            msg_key = (message.get('partition'), message.get('offset'))
            if msg_key in self.processed_messages:
                logger.debug(f"Skipping duplicate message: partition={msg_key[0]}, offset={msg_key[1]}")
                return True
            self.processed_messages.add(msg_key)
            # Keep only last 1000 message keys to prevent memory growth
            if len(self.processed_messages) > 1000:
                # Remove oldest entries (simple approach: clear and rebuild on next cycle)
                self.processed_messages = set(list(self.processed_messages)[-500:])
            
            value = message.get('value', {})
            data = value.get('data', {})
            
            # Only process message.received events
            event_type = value.get('event_type')
            logger.debug(f"Received event type: {event_type}, full message: {message}")
            if event_type != 'message.received':
                logger.warning(f"Skipping non-message event: {event_type}, value keys: {list(value.keys())}")
                return True

            text = data.get('text', '').strip()
            from_phone = data.get('from_phone')
            chat_id = self.api.get_chat_id(value)
            logger.info(f"Processing message - from: {from_phone}, text: {text[:50]}, chat_id: {chat_id}")

            if not from_phone:
                return False

            # Skip our own messages
            if from_phone == self.sender_number:
                return True

            # Ensure message is for our number if chat_handles are provided
            chat_handles = data.get('chat_handles', [])
            if chat_handles:
                our_number_in_chat = any(
                    (handle or {}).get('identifier') == self.sender_number
                    for handle in chat_handles
                )
            else:
                # Some events may not include chat_handles; process them
                logger.debug("No chat_handles provided; processing message")
                our_number_in_chat = True
            if not our_number_in_chat:
                logger.warning(f"Skipping message not for our number. chat_handles={chat_handles}")
                return True
            
            logger.info(f"Processing message for {self.sender_number}")

            if not text:
                return True

            # Store chat_id in both systems (critical for routing)
            if chat_id:
                self.switchboard.store_chat_id(from_phone, chat_id)
                self.state.store_chat_id(from_phone, chat_id)
                logger.info(f"Stored chat_id {chat_id} for {from_phone}")
            else:
                logger.warning(f"No chat_id in message from {from_phone} - this may cause issues")

            logger.info(f"Message from {from_phone}: {text[:50]}")

            # Check for command
            cmd = self.commands.parse_command(text)
            if cmd:
                return self.handle_command(from_phone, chat_id, cmd[0], cmd[1])

            # Handle based on user status
            status = self.state.get_status(from_phone)
            
            if status == Status.ONBOARDING_NAME:
                return self.handle_onboarding_name(from_phone, chat_id, text)
            elif status == Status.ONBOARDING_EMAIL:
                return self.handle_onboarding_email(from_phone, chat_id, text)
            elif status == Status.ONBOARDING_INTRO or status == Status.ONBOARDING:
                return self.handle_onboarding_intro(from_phone, chat_id, text)
            elif status == Status.CHATTING:
                return self.handle_chat_message(from_phone, chat_id, text)
            else:
                # Default: prompt for onboarding or match
                if status == Status.IDLE:
                    return self.start_onboarding(from_phone, chat_id)
                else:
                    self.send(chat_id, from_phone, "Type /match to find someone to chat with, or /help for commands.")
                return True

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            return False

    def handle_command(self, phone: str, chat_id: Optional[int], cmd: str, arg: Optional[str]) -> bool:
        """Handle a command."""
        logger.info(f"Command from {phone}: {cmd} {arg or ''}")

        if cmd == 'match':
            status = self.state.get_status(phone)
            
            if status == Status.CHATTING:
                self.send(chat_id, phone, "You're already in a conversation! Type /end to disconnect.")
                return True
            
            if status in (Status.ONBOARDING, Status.ONBOARDING_NAME, Status.ONBOARDING_EMAIL, Status.ONBOARDING_INTRO):
                self.send(chat_id, phone, "Please finish setting up your profile first, then send /match.")
                return True
            
            # Prevent duplicate match requests
            if status == Status.SEARCHING:
                self.send(chat_id, phone, "Already searching for a match...")
                return True
            
            if status != Status.READY:
                # Ensure they have an intro
                if not self.state.get_intro(phone):
                    return self.start_onboarding(phone, chat_id)
                self.state.set_status(phone, Status.READY)
            
            # Check waiting queue
            partner = self.state.pop_from_queue()
            
            if partner:
                # Match found!
                self.match_two_users(phone, partner)
            else:
                # Add to queue
                self.state.set_status(phone, Status.SEARCHING)
                self.state.add_to_queue(phone)
                self.send(chat_id, phone, "Searching for a match...")
            
            return True

        elif cmd == 'card':
            return self.handle_card(phone, chat_id)
        
        elif cmd == 'end':
            partner = self.state.get_partner(phone)
            if partner:
                # Clear from both systems
                self.state.clear_partner(phone)
                self.switchboard.end_chat(phone)
                self.state.set_status(phone, Status.READY)
                self.state.set_status(partner, Status.READY)
                
                partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
                if partner_chat:
                    self.send(partner_chat, partner, "Stranger disconnected. Type /match to find someone new.")
                self.send(chat_id, phone, "Disconnected. Type /match to find someone new.")
            else:
                self.send(chat_id, phone, "You're not connected to anyone.")
            return True

        elif cmd == 'reveal':
            if not self.state.get_partner(phone) and not self.switchboard.is_paired(phone):
                self.send(chat_id, phone, "You're not connected. Type /match first.")
                return True

            both, partner = self.switchboard.handle_reveal(phone)
            if both and partner:
                self.exchange_contacts(phone, partner)
            elif partner:
                self.send(chat_id, phone, "Waiting for partner to accept...")
                partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
                if partner_chat:
                    self.send(partner_chat, partner, "Partner wants to share contact! Type /reveal to accept.")
            return True

        elif cmd == 'we':
            return self.handle_we(phone, chat_id)
        elif cmd == 'mood':
            return self.handle_mood(phone, chat_id)
        elif cmd == 'topics':
            return self.handle_topics(phone, chat_id)
        elif cmd == 'override':
            # Hidden testing shortcut: skip onboarding and mark ready
            if not self.state.get_intro(phone):
                self.state.set_intro(phone, "Skipped setup (testing).")
            self.state.set_status(phone, Status.READY)
            self.send(chat_id, phone, "Setup skipped (testing).")
            return True
        elif cmd == 'unknown':
            self.send(chat_id, phone, "I don't recognize that command. Type /help for a list of commands.")
            return True
        elif cmd == 'setcity':
            return self.handle_setcity(phone, chat_id, arg)
        elif cmd == 'cityyes':
            return self.handle_city_confirm(phone, chat_id, approved=True)
        elif cmd == 'cityno':
            return self.handle_city_confirm(phone, chat_id, approved=False)
        elif cmd == 'icebreaker':
            return self.handle_icebreaker(phone, chat_id)
        elif cmd == 'icebreakers':
            return self.handle_icebreakers(phone, chat_id)
        elif cmd == 'bucketlist':
            return self.handle_bucketlist(phone, chat_id)
        elif cmd == 'spotify':
            return self.handle_spotify(phone, chat_id)
        elif cmd == 'spotifylink':
            return self.handle_spotifylink(phone, chat_id, arg)
        elif cmd == 'quiz':
            return self.handle_quiz(phone, chat_id, arg)
        elif cmd == 'topspots':
            return self.handle_topspots(phone, chat_id)

        else:
            response = self.commands.execute_command(self.switchboard, phone, cmd, arg)
            if response:
                self.send(chat_id, phone, response)
            return True

    def start_onboarding(self, user_id: str, chat_id: Optional[int]) -> bool:
        """Start onboarding for a new user."""
        self.state.set_status(user_id, Status.ONBOARDING_NAME)
        welcome_msg = """Welcome! I help you meet someone and slowly build a profile of them based only on your chat.

First, what's your name?"""
        self.send(chat_id, user_id, welcome_msg)
        return True

    def handle_onboarding_name(self, user_id: str, chat_id: Optional[int], text: str) -> bool:
        """Handle name input during onboarding."""
        name = normalize_name_input(text)

        if not name:
            self.send(chat_id, user_id, "I didn't quite catch that. Try replying with just your name, e.g. 'Alana'.")
            return True

        if len(name.split()) > 4:
            self.send(chat_id, user_id, "Try just sending your name, for example: 'Alana Kwan'.")
            return True

        if len(name) > 50:
            self.send(chat_id, user_id, "Please enter a short name using letters only (e.g., Alana or Alana Smith).")
            return True
        
        # Store name in profile
        self.switchboard.store_user_profile(user_id, {'name': name})
        self.state.set_status(user_id, Status.ONBOARDING_EMAIL)
        
        self.send(chat_id, user_id, f"Nice to meet you, {name}! What's your email? (This will be shared when you /reveal)")
        logger.info(f"User {user_id} set name: {name}")
        return True

    def handle_onboarding_email(self, user_id: str, chat_id: Optional[int], text: str) -> bool:
        """Handle email input during onboarding."""
        email = text.strip()
        if '@' not in email or '.' not in email:
            self.send(chat_id, user_id, "Please enter a valid email address.")
            return True
        
        # Store email in profile
        self.switchboard.store_user_profile(user_id, {'email': email})
        self.state.set_status(user_id, Status.ONBOARDING_INTRO)
        
        self.send(chat_id, user_id, "Great! Now send a short intro about yourself and what you're looking for.")
        logger.info(f"User {user_id} set email: {email}")
        return True

    def handle_onboarding_intro(self, user_id: str, chat_id: Optional[int], text: str) -> bool:
        """Handle intro input during onboarding."""
        if text.strip().startswith('/'):
            self.send(
                chat_id,
                user_id,
                "That looks like a command. For your intro, send a short blurb about yourself without '/'."
            )
            return True
        # Store intro
        self.state.set_intro(user_id, text)
        self.state.set_status(user_id, Status.READY)
        
        response = """Got it! Your profile is set up.

When you want to meet someone, send /match.
Send /help for all commands."""
        self.send(chat_id, user_id, response)
        logger.info(f"User {user_id} completed onboarding")
        return True

    def handle_onboarding(self, user_id: str, chat_id: Optional[int], text: str) -> bool:
        """Legacy handler - redirects to intro handler."""
        return self.handle_onboarding_intro(user_id, chat_id, text)

    def match_two_users(self, user_a: str, user_b: str):
        """Match two users."""
        logger.info(f"Matching {user_a} with {user_b}")
        
        # Prevent duplicate matches - check if already matched
        if self.state.get_partner(user_a) == user_b:
            logger.info(f"Users {user_a} and {user_b} already matched, skipping")
            return
        
        chat_a = self.state.get_chat_id(user_a) or self.switchboard.get_chat_id(user_a)
        chat_b = self.state.get_chat_id(user_b) or self.switchboard.get_chat_id(user_b)
        
        # Do not enter chatting state without valid chat_ids for both users
        if chat_a is None or chat_b is None:
            logger.error(f"Cannot match {user_a} and {user_b}: missing chat_id(s) (chat_a={chat_a}, chat_b={chat_b})")
            self.state.set_status(user_a, Status.READY)
            self.state.set_status(user_b, Status.READY)
            if chat_a:
                self.send(chat_a, user_a, "I had trouble connecting your chat. Try sending /match again.")
            if chat_b:
                self.send(chat_b, user_b, "I had trouble connecting your chat. Try sending /match again.")
            return
        
        # Set partners and status now that chat_ids are confirmed
        self.state.set_partner(user_a, user_b)
        self.state.set_status(user_a, Status.CHATTING)
        self.state.set_status(user_b, Status.CHATTING)
        
        # Also update switchboard for compatibility
        self.switchboard.active_pairs[user_a] = user_b
        self.switchboard.active_pairs[user_b] = user_a
        
        # Send connection messages - use send() which will retrieve chat_id from stored state
        # This avoids creating new chats if chat_id wasn't passed
        msg = "Connected! Say hi. (Type /end to disconnect, /card to see their profile)"
        
        self.send(chat_a, user_a, msg)
        self.send(chat_b, user_b, msg)
        logger.info(f"Matched {user_a} with {user_b}")

    def handle_chat_message(self, user_id: str, chat_id: Optional[int], text: str) -> bool:
        """Handle a chat message between matched users."""
        start_ts = time.time()
        partner = self.state.get_partner(user_id)
        if not partner:
            self.state.set_status(user_id, Status.READY)
            self.send(chat_id, user_id, "You're not in a conversation. Send /match to meet someone.")
            return True
        
        # Handle active quiz - treat message as a guess
        if self.quiz_game and self.quiz_game.has_active_quiz(user_id):
            return self.handle_quiz_guess(user_id, chat_id, text)

        # Handle in-chat icebreaker flow before relaying messages
        if self.state.icebreaker_active(user_id, partner):
            return self.handle_icebreakers_message(user_id, partner, chat_id, text)
        # Handle in-chat bucketlist flow before relaying messages
        if self.state.is_bucketlist_active(user_id, partner):
            return self.handle_bucketlist_message(user_id, partner, chat_id, text)

        # If the last message to this user was an AI prompt, skip AI follow-ups for this reply.
        skip_ai = False
        if user_id in self.skip_ai_after_prompt:
            skip_ai = True
            self.skip_ai_after_prompt.discard(user_id)

        # Capture partner last message time before we record this one
        partner_last_ts = self.state.get_last_message_ts_for_user(partner)

        # Record message and capture last message ts
        self.state.record_message(user_id, partner, text)
        
        # Forward to partner ASAP
        partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
        if partner_chat:
            self.send(partner_chat, partner, text)
        else:
            logger.error(f"Cannot relay message to {partner}: chat_id not found")
            return False
        
        # Kick off profile update in the background so it doesn't block sending
        def _update_profiles_async(chat_id_self: Optional[int], chat_id_partner: Optional[int], skip_ai_flag: bool):
            try:
                total_messages = self.state.get_message_count(user_id, partner)
                new_level_self, old_level_self, changed_self = self.ai.ensure_profile_updated(
                    self.state, user_id, partner, total_messages
                )
                new_level_partner, old_level_partner, changed_partner = self.ai.ensure_profile_updated(
                    self.state, partner, user_id, total_messages
                )

                if not skip_ai_flag:
                    if changed_self and chat_id_self:
                        self.send(chat_id_self, user_id, f"Profile level {new_level_self} unlocked. Use /card to see more.")
                    if changed_partner and chat_id_partner:
                        self.send(chat_id_partner, partner, f"Profile level {new_level_partner} unlocked. Use /card to see more.")

                logger.info(f"Async profile update done for {user_id} <-> {partner} (total {total_messages})")
            except Exception as e:
                logger.error(f"Async profile update error: {e}", exc_info=True)
        
        threading.Thread(target=_update_profiles_async, args=(chat_id, partner_chat, skip_ai), daemon=True).start()
        
        logger.info(f"Relayed message from {user_id} to {partner} in {time.time() - start_ts:.3f}s")
        return True

    def handle_we(self, user_id: str, chat_id: Optional[int]) -> bool:
        """Handle /we command - show shared profile of the conversation."""
        partner = self.state.get_partner(user_id)
        if not partner:
            self.send(chat_id, user_id, "You are not in a conversation. Send /match to meet someone.")
            return True

        conversation = self.state.get_conversation(user_id, partner)
        summary = self.ai.generate_shared_profile(conversation)
        self.send(chat_id, user_id, "Here's what I notice about the two of you:\n\n" + summary)
        return True

    def handle_mood(self, user_id: str, chat_id: Optional[int]) -> bool:
        """Handle /mood command."""
        partner = self.state.get_partner(user_id)
        if not partner:
            self.send(chat_id, user_id, "You are not in a conversation. Send /match to meet someone.")
            return True
        conversation = self.state.get_conversation(user_id, partner)
        if not conversation:
            self.send(chat_id, user_id, "I need a few messages first to read the mood.")
            return True
        mood = self.ai.generate_mood(conversation)
        self.send(chat_id, user_id, f"Current vibe: {mood}")
        return True

    def handle_topics(self, user_id: str, chat_id: Optional[int]) -> bool:
        """Handle /topics command."""
        partner = self.state.get_partner(user_id)
        if not partner:
            self.send(chat_id, user_id, "You are not in a conversation. Send /match to meet someone.")
            return True
        conversation = self.state.get_conversation(user_id, partner)
        if not conversation:
            self.send(chat_id, user_id, "I need some messages to extract topics.")
            return True
        topics = self.ai.generate_topics(conversation)
        self.send(chat_id, user_id, "Topics you've covered:\n" + topics)
        return True

    def handle_icebreakers(self, phone: str, chat_id: Optional[int]) -> bool:
        """Start in-chat Two Truths and a Lie (no web UI)."""
        partner = self.state.get_partner(phone)
        if not partner:
            self.send(chat_id, phone, "You need to be in a conversation first. Type /match to find someone.")
            return True

        self.state.start_icebreaker(phone, partner)

        partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)

        instructions = (
            "🎲 Let's play Two Truths and a Lie right here.\n"
            "Send ONE message with THREE lines: truth, truth, LIE (lie goes LAST).\n"
            "Example:\n"
            "I ran a marathon\n"
            "I speak Italian\n"
            "I have never flown on a plane"
        )

        self.send(chat_id, phone, instructions)
        if partner_chat:
            self.send(
                partner_chat,
                partner,
                "🎲 Your partner started Two Truths and a Lie.\n"
                "Reply with three lines in one message: truth, truth, LIE (lie last).\n"
                "When both of you send yours, I'll share them."
            )
        else:
            logger.warning(f"No chat_id for partner {partner} when starting in-chat icebreaker.")

        return True

    def handle_icebreakers_message(self, user_id: str, partner: str, chat_id: Optional[int], text: str) -> bool:
        """Capture statements for the in-chat icebreaker and share once both respond."""
        if not text.strip():
            self.send(chat_id, user_id, "Please send three lines (truth, truth, lie last) in one message.")
            return True

        ib = self.state.get_icebreaker(user_id, partner)
        statements_map = ib.get("statements", {})

        parsed = parse_statements(text)
        parsed = [p for p in parsed if p]
        if len(parsed) < 3:
            self.send(
                chat_id,
                user_id,
                "I need at least three lines: truth, truth, lie (lie last). "
                "Please resend all three in one message."
            )
            return True

        statements = parsed[:3]
        # Allow overwrite until both have submitted
        self.state.set_icebreaker_statements(user_id, partner, statements, lie_index=2)
        preview = "\n".join(f"{idx+1}. {s}" for idx, s in enumerate(statements))
        self.send(
            chat_id,
            user_id,
            "Saved your two truths and a lie:\n"
            f"{preview}\n\n"
            "If that's wrong, just resend all three lines."
        )

        ib = self.state.get_icebreaker(user_id, partner)
        if len(ib.get("statements", {})) == 2:
            partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
            partner_statements = ib["statements"].get(partner)
            user_statements = ib["statements"].get(user_id)

            if partner_statements and chat_id:
                self.send(chat_id, user_id, format_partner_share(partner_statements))
            if user_statements and partner_chat:
                self.send(partner_chat, partner, format_partner_share(user_statements))
            elif not partner_chat:
                logger.warning(f"Could not share icebreaker statements with {partner}: missing chat_id")

            self.state.clear_icebreaker(user_id, partner)

        return True

    def handle_icebreaker(self, phone: str, chat_id: Optional[int]) -> bool:
        """Start web-based icebreaker session and share links."""
        partner = self.state.get_partner(phone)
        if not partner:
            self.send(chat_id, phone, "You need to be in a conversation first. Type /match to find someone.")
            return True

        # Generate or reuse session_id for the pair
        session_id = self.state.get_icebreaker_session(phone, partner)
        if not session_id:
            session_id = uuid.uuid4().hex
            self.state.set_icebreaker_session(phone, partner, session_id)

        base_url = os.getenv("ICEBREAKER_BASE_URL", "https://your-game-host")
        url_self = f"{base_url}/game?session={session_id}&who=me"
        url_partner = f"{base_url}/game?session={session_id}&who=partner"

        self.send(
            chat_id,
            phone,
            "🎮 I set up an icebreaker game for you two.\n"
            "Tap this link to play together, then come back here to keep chatting:\n"
            f"{url_self}"
        )

        partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
        if partner_chat:
            self.send(
                partner_chat,
                partner,
                "🎮 Your partner started an icebreaker game.\n"
                "Tap this link to join, then come back here to keep chatting:\n"
                f"{url_partner}"
            )
        return True

    def handle_bucketlist(self, phone: str, chat_id: Optional[int]) -> bool:
        """Start in-chat Shared Bucket List Builder."""
        partner = self.state.get_partner(phone)
        if not partner:
            self.send(chat_id, phone, "You need to be in a conversation first. Type /match to find someone.")
            return True

        # Prompt for city if missing so real-world recs can run
        profile_me = self.switchboard.get_user_profile(phone)
        profile_partner = self.switchboard.get_user_profile(partner)
        city = profile_me.get("city") or profile_partner.get("city")
        if not city:
            city_hint = "Set a city with /setcity <City> to get real place recs in this chat."
            self.send(chat_id, phone, city_hint)
            partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
            if partner_chat:
                self.send(partner_chat, partner, city_hint)

        self.state.set_bucketlist_active(phone, partner, True)

        instructions = (
            "📝 Shared Bucket List Builder\n"
            "Send ONE message with 5 lines:\n"
            "1) Travel\n2) Skill to learn\n3) Food experience\n4) Adventure\n5) Creative experience\n\n"
            "You can type 'skip' on any line if you're not sure."
        )

        self.send(chat_id, phone, instructions)

        partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
        if partner_chat:
            self.send(
                partner_chat,
                partner,
                "📝 Your partner started a Shared Bucket List.\n"
                "Reply with 5 lines in one message: travel, skill, food, adventure, creative.\n"
                "You can type 'skip' on any line."
            )
        else:
            logger.warning(f"No chat_id for partner {partner} when starting in-chat bucketlist.")

        return True

    def handle_bucketlist_message(self, user_id: str, partner: str, chat_id: Optional[int], text: str) -> bool:
        """Capture bucket list entries and share merged list when both respond."""
        if not text.strip():
            self.send(chat_id, user_id, "Please send 5 lines (travel, skill, food, adventure, creative).")
            return True

        items = parse_bucketlist(text)
        non_empty = [v for v in items.values() if v and v.strip().lower() != "skip"]
        if len(non_empty) < 3:
            self.send(
                chat_id,
                user_id,
                "I need at least a few items. Please send 5 lines (you can use 'skip' for any)."
            )
            return True

        # Save/overwrite user's submission
        self.state.set_bucketlist(user_id, partner, items)
        self.send(chat_id, user_id, format_captured(items) + "\n\nIf that's wrong, resend all 5 lines.")

        pair_entries = self.state.get_bucketlist_pair(user_id, partner)
        if len(pair_entries) == 2:
            partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
            me_items = pair_entries.get(user_id, {})
            partner_items = pair_entries.get(partner, {})
            merged = format_shared_bucketlist(me_items, partner_items)

            # Persist shared bucket list on both user profiles so it survives restarts
            try:
                prof_me = self.switchboard.get_user_profile(user_id) or {}
                prof_partner = self.switchboard.get_user_profile(partner) or {}
                bucket_key = "bucketlist_with"
                prof_me.setdefault(bucket_key, {})
                prof_partner.setdefault(bucket_key, {})
                prof_me[bucket_key][partner] = {"self": me_items, "partner": partner_items}
                prof_partner[bucket_key][user_id] = {"self": partner_items, "partner": me_items}
                self.switchboard.store_user_profile(user_id, prof_me)
                self.switchboard.store_user_profile(partner, prof_partner)
            except Exception as e:
                logger.error(f"Failed to persist bucketlist for {user_id} & {partner}: {e}", exc_info=True)

            if chat_id:
                self.send(chat_id, user_id, merged)
            if partner_chat:
                self.send(partner_chat, partner, merged)
            else:
                logger.warning(f"Could not deliver merged bucketlist to {partner}: missing chat_id")

            # AI suggestions based on both lists
            try:
                suggestions = self.ai.generate_bucketlist_suggestions(me_items, partner_items)
                suggestions_text = (
                    "Here are a few things you could actually do together based on your lists:\n"
                    f"{suggestions}\n\n"
                    "Reply /topspots if you want me to find top-rated real places that match these ideas."
                )
                # Store plan text so /topspots can read it later
                self.state.set_shared_bucketlist_plan(user_id, partner, suggestions_text)
                if chat_id:
                    self.send(chat_id, user_id, suggestions_text)
                if partner_chat:
                    self.send(partner_chat, partner, suggestions_text)
            except Exception as e:
                logger.error(f"Bucketlist suggestions error: {e}", exc_info=True)

            # Real-world places recommendations (requires city and Google Maps API key)
            profile_me = self.switchboard.get_user_profile(user_id)
            profile_partner = self.switchboard.get_user_profile(partner)
            city = profile_me.get("city") or profile_partner.get("city")
            if city:
                try:
                    places = get_real_world_recommendations(me_items, partner_items, city)
                    if places:
                        lines = [f"📍 Real places in {city} that match your bucket list:"]
                        label_map = {
                            "travel": "Travel",
                            "experience": "Experience",
                            "food": "Food",
                            "shrine": "Shrine",
                            "ski": "Ski",
                            "onsen": "Onsen",
                            "shopping": "Shopping",
                            "class": "Class",
                            "attraction": "Attraction",
                            "other": "Idea",
                        }
                        for place in places:
                            ptype = (place.get("type", "") or "").lower()
                            label = label_map.get(ptype, ptype.capitalize() or "Idea")
                            name = place.get("name", "")
                            addr = place.get("address", "")
                            url = place.get("url", "")
                            line = f"- [{label}] {name}"
                            if addr:
                                line += f" — {addr}"
                            if url:
                                line += f" — {url}"
                            lines.append(line)
                        lines += [
                            "",
                            "Reply /topspots if you want one top-rated place per idea,",
                            "with rating and why it fits your bucket list."
                        ]
                        real_text = "\n".join(lines)
                        if chat_id:
                            self.send(chat_id, user_id, real_text)
                        if partner_chat:
                            self.send(partner_chat, partner, real_text)
                    else:
                        notice = (
                            "I couldn't find real places right now. "
                            "If you set your city with /setcity and have a valid Google Maps API key, try again in a bit."
                        )
                        if chat_id:
                            self.send(chat_id, user_id, notice)
                        if partner_chat:
                            self.send(partner_chat, partner, notice)
                except Exception as e:
                    logger.error(f"Real-world bucketlist recs error: {e}", exc_info=True)
            else:
                logger.info("No city set for pair; skipping real-world bucketlist recommendations.")
                notice = (
                    "To get real place recs, set a city with /setcity <City>. "
                    "Real-places step runs only when at least one profile has a city."
                )
                if chat_id:
                    self.send(chat_id, user_id, notice)
                if partner_chat:
                    self.send(partner_chat, partner, notice)

            # Stop intercepting further messages for bucket list flow, but keep entries for /topspots
            self.state.set_bucketlist_active(user_id, partner, False)

        return True

    def handle_topspots(self, phone: str, chat_id: Optional[int]) -> bool:
        """Provide top-rated places per bucketlist theme on demand."""
        partner = self.state.get_partner(phone)
        if not partner:
            # Try to recover partner from stored bucketlist (e.g., after restart)
            partner = self.state.find_partner_from_bucketlist(phone)
            if not partner:
                # Try from cached profile bucketlist_with
                try:
                    prof_self = self.switchboard.get_user_profile(phone) or {}
                    bucket_key = "bucketlist_with"
                    cached = prof_self.get(bucket_key, {})
                    if cached:
                        partner = next(iter(cached.keys()))
                        cached_entry = cached.get(partner, {})
                        if cached_entry:
                            # Rehydrate state so downstream logic works
                            self.state.set_bucketlist(phone, partner, cached_entry.get("self", {}))
                            self.state.set_bucketlist(partner, phone, cached_entry.get("partner", {}))
                except Exception as e:
                    logger.error(f"Failed to recover partner for topspots: {e}", exc_info=True)

            if partner:
                # Restore chatting state so we don't block the command
                self.state.set_partner(phone, partner)
                self.state.set_status(phone, Status.CHATTING)
                self.state.set_status(partner, Status.CHATTING)
            else:
                self.send(chat_id, phone, "You need to be in a conversation first.")
                return True

        pair_entries = self.state.get_bucketlist_pair(phone, partner)
        # If not in memory (e.g., after restart), try to recover from profiles
        if not pair_entries:
            try:
                prof_self = self.switchboard.get_user_profile(phone) or {}
                prof_partner = self.switchboard.get_user_profile(partner) or {}
                bucket_key = "bucketlist_with"
                cached = prof_self.get(bucket_key, {}).get(partner)
                if cached:
                    # cached has shape {"self": ..., "partner": ...} from perspective of phone
                    self.state.set_bucketlist(phone, partner, cached.get("self", {}))
                    self.state.set_bucketlist(partner, phone, cached.get("partner", {}))
                    pair_entries = self.state.get_bucketlist_pair(phone, partner)
                elif prof_partner.get(bucket_key, {}).get(phone):
                    cached_rev = prof_partner[bucket_key][phone]
                    self.state.set_bucketlist(phone, partner, cached_rev.get("partner", {}))
                    self.state.set_bucketlist(partner, phone, cached_rev.get("self", {}))
                    pair_entries = self.state.get_bucketlist_pair(phone, partner)
            except Exception as e:
                logger.error(f"Failed to restore bucketlist for {phone} & {partner}: {e}", exc_info=True)

        me_items = pair_entries.get(phone)
        partner_items = pair_entries.get(partner)
        # City from either profile
        profile_self = self.switchboard.get_user_profile(phone)
        profile_partner = self.switchboard.get_user_profile(partner)
        city = (profile_self.get("city") or profile_partner.get("city") or "").strip()
        if not city:
            self.send(chat_id, phone, "Set your city first so I can find real places. Use /setcity <city>.")
            return True

        # Shared plan text (AI suggestions) stored when bucketlist merged
        shared_plan = self.state.get_shared_bucketlist_plan(phone, partner)
        if not shared_plan and me_items and partner_items:
            # Fallback: regenerate plan so we can build themes
            try:
                shared_plan = (
                    "Here are a few things you could actually do together based on your lists:\n"
                    f"{self.ai.generate_bucketlist_suggestions(me_items, partner_items)}"
                )
                self.state.set_shared_bucketlist_plan(phone, partner, shared_plan)
            except Exception as e:
                logger.error(f"Failed to regenerate shared plan for topspots: {e}", exc_info=True)

        if not shared_plan:
            self.send(chat_id, phone, "I don't have a shared plan yet. Run /bucketlist first and then try /topspots.")
            return True

        # Themes from plan text
        themes = self.ai.generate_bucketlist_themes_from_plan(shared_plan)
        if not themes:
            self.send(chat_id, phone, "I couldn't read your plan to build real-world searches. Try /topspots again.")
            return True

        try:
            spots = get_top_spots_for_themes(themes, city)
        except Exception as e:
            logger.error(f"Error computing top spots for {phone}: {e}", exc_info=True)
            self.send(chat_id, phone, "I had trouble finding specific places just now.")
            return True

        if not spots:
            self.send(chat_id, phone, f"I couldn't find good top-rated matches in {city} right now.")
            return True

        lines = [f"🎯 Top picks for your shared bucket list in {city}:\n"]
        for spot in spots:
            label = spot.get("theme_label", "Idea")
            place = spot.get("place", {}) or {}
            name = place.get("name", "")
            addr = place.get("address", "")
            rating = place.get("rating")
            url = place.get("url", "") or place.get("googleMapsUri", "")
            why = spot.get("why", "")
            why_popular = spot.get("why_popular", "")

            lines.append(f"• {label}:")
            core = f"  {name}"
            if rating:
                core += f" ({float(rating):.1f}★)"
            lines.append(core)
            if why:
                lines.append(f"  {why}")
            if why_popular:
                lines.append(f"  {why_popular}")
            if addr:
                lines.append(f"  {addr}")
            if url:
                lines.append(f"  {url}")
            lines.append("")  # spacer

        msg = "\n".join(lines).strip()

        chat_self = chat_id or self.state.get_chat_id(phone) or self.switchboard.get_chat_id(phone)
        chat_partner = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)

        if chat_self:
            self.send(chat_self, phone, msg)
        if chat_partner:
            self.send(chat_partner, partner, msg)

        return True

    def _build_spotify_auth_url(self, state: str) -> Optional[str]:
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")
        scope = "user-read-private user-read-email"
        if not client_id or not redirect_uri:
            return None
        params = {
            "client_id": client_id,
            "response_type": "code",
            "redirect_uri": redirect_uri,
            "scope": scope,
            "state": state,
        }
        return "https://accounts.spotify.com/authorize?" + urllib.parse.urlencode(params)

    def handle_spotify(self, phone: str, chat_id: Optional[int]) -> bool:
        """Send a Spotify auth link for the user to connect their account."""
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")
        if not client_id or not redirect_uri:
            self.send(chat_id, phone, "Spotify is not configured yet. Set SPOTIFY_CLIENT_ID and SPOTIFY_REDIRECT_URI.")
            return True

        state = uuid.uuid4().hex
        url = self._build_spotify_auth_url(state)
        if not url:
            self.send(chat_id, phone, "I couldn't generate a Spotify auth link. Please try again later.")
            return True

        # Store state on the profile for later validation
        profile = self.switchboard.get_user_profile(phone)
        profile["spotify_state"] = state
        self.switchboard.store_user_profile(phone, profile)

        self.send(
            chat_id,
            phone,
            "Connect your Spotify account:\n"
            f"{url}\n\n"
            "After you approve, I'll link your account here."
        )
        return True

    def handle_setcity(self, phone: str, chat_id: Optional[int], arg: Optional[str]) -> bool:
        """Set city; if paired, request partner confirmation."""
        if not arg or not arg.strip():
            self.send(chat_id, phone, "Usage: /setcity <city>")
            return True
        city = arg.strip()
        partner = self.state.get_partner(phone)
        if not partner:
            # Not paired; set directly
            self.switchboard.store_user_profile(phone, {"city": city})
            self.send(chat_id, phone, f"City set to: {city}")
            return True

        # Paired: request confirmation from partner
        self.state.set_pending_city(phone, partner, city, set_by=phone)
        self.send(chat_id, phone, f"Requested city set to: {city}. Waiting for your partner to confirm (/cityyes or /cityno).")
        partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
        if partner_chat:
            self.send(
                partner_chat,
                partner,
                f"Your partner wants to set the city to: {city}. Reply /cityyes to accept or /cityno to decline."
            )
        return True

    def handle_city_confirm(self, phone: str, chat_id: Optional[int], approved: bool) -> bool:
        """Handle /cityyes or /cityno from partner."""
        partner = self.state.get_partner(phone)
        if not partner:
            self.send(chat_id, phone, "You need to be in a conversation first.")
            return True
        pending = self.state.get_pending_city(phone, partner)
        if not pending:
            self.send(chat_id, phone, "No city change pending.")
            return True

        city = pending.get("city", "")
        set_by = pending.get("set_by", "")
        partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)

        if approved:
            # Apply city to both users
            self.switchboard.store_user_profile(phone, {"city": city})
            self.switchboard.store_user_profile(partner, {"city": city})
            self.state.clear_pending_city(phone, partner)
            msg_self = f"City set to: {city} (confirmed)."
            msg_partner = f"City set to: {city} (you confirmed)."
            if chat_id:
                self.send(chat_id, phone, msg_self)
            if partner_chat:
                self.send(partner_chat, partner, msg_partner)
        else:
            self.state.clear_pending_city(phone, partner)
            decline_self = "City change declined. You can propose another with /setcity <city>."
            decline_partner = "City change declined. Propose another city with /setcity <city>."
            if chat_id:
                self.send(chat_id, phone, decline_self)
            if partner_chat:
                self.send(partner_chat, partner, decline_partner)
        return True

    def handle_card(self, user_id: str, chat_id: Optional[int]) -> bool:
        """Handle /card command - show mini profile + interests of the partner."""
        partner = self.state.get_partner(user_id)
        
        if not partner:
            self.send(chat_id, user_id, "You are not in a conversation. Send /match to meet someone.")
            return True
        
        profile = self.state.get_profile(user_id, partner)
        conversation = self.state.get_conversation(user_id, partner)
        topics = ""
        if conversation:
            topics = self.ai.generate_topics(conversation)
        
        if profile["level"] == 0:
            body = "I have not learned enough yet. Keep talking to unlock their profile."
        else:
            body = (
                "Here is what I have learned about them from your conversation so far:\n\n"
                f"{profile['resume']}"
            )
        
        if topics:
            body += "\n\nInterests you've been talking about:\n" + topics
        
        self.send(chat_id, user_id, body)
        return True

    def exchange_contacts(self, user1: str, user2: str) -> bool:
        """Exchange vCards between users."""
        try:
            profile1 = self.switchboard.get_user_profile(user1)
            profile2 = self.switchboard.get_user_profile(user2)

            # Ensure names
            if not profile1.get('name'):
                profile1['name'] = f"User {user1[-4:]}"
            if not profile2.get('name'):
                profile2['name'] = f"User {user2[-4:]}"

            vcard1 = generate_vcard(profile1)
            vcard2 = generate_vcard(profile2)

            chat1 = self.switchboard.get_chat_id(user1)
            chat2 = self.switchboard.get_chat_id(user2)

            # Send vCards
            self.send(chat1, user1, "Contact shared!", vcard2, "contact.vcf", "text/vcard")
            self.send(chat2, user2, "Contact shared!", vcard1, "contact.vcf", "text/vcard")

            logger.info(f"Exchanged contacts: {user1} <-> {user2}")
            return True

        except Exception as e:
            logger.error(f"Error exchanging contacts: {e}")
            return False

    # ==================== Music Quiz Feature ====================
    
    def handle_spotify(self, phone: str, chat_id: Optional[int]) -> bool:
        """Handle /spotify command - link Spotify account."""
        if not self.quiz_game:
            self.send(chat_id, phone, "Music quiz feature is not available. Missing API configuration.")
            return True
        
        # Check if already linked
        if self.quiz_game.spotify.has_token(phone):
            self.send(chat_id, phone, "Your Spotify is already linked! Use /quiz to start a music game.")
            return True
        
        # Generate auth URL
        auth_url = self.quiz_game.spotify.get_auth_url(phone)
        if auth_url:
            self.send(
                chat_id, 
                phone, 
                f"Click this link to connect your Spotify:\n\n{auth_url}\n\nAfter authorizing, you can use /quiz to play!"
            )
        else:
            self.send(chat_id, phone, "Couldn't generate Spotify link. Try again later.")
        
        return True
    
    def handle_spotifylink(self, phone: str, chat_id: Optional[int], arg: Optional[str]) -> bool:
        """Handle /spotifylink command - complete Spotify auth with code from Vercel callback."""
        if not self.quiz_game:
            self.send(chat_id, phone, "Music quiz feature is not available.")
            return True
        
        if not arg:
            self.send(chat_id, phone, "Missing link code. Use /spotify to start fresh.")
            return True
        
        try:
            import base64
            import json
            # Decode the payload from Vercel callback
            payload = json.loads(base64.b64decode(arg).decode('utf-8'))
            state = payload.get('s')
            code = payload.get('c')
            
            if not state or not code:
                raise ValueError("Invalid payload")
            
            # Exchange code for token
            user_phone = self.quiz_game.spotify.handle_callback(code, state)
            
            if user_phone:
                self.send(chat_id, phone, "Spotify linked successfully! Use /quiz to start a music game with your partner.")
            else:
                self.send(chat_id, phone, "Couldn't complete Spotify link. Try /spotify again.")
                
        except Exception as e:
            logger.error(f"Error in spotifylink: {e}")
            self.send(chat_id, phone, "Invalid link code. Use /spotify to start fresh.")
        
        return True
    
    def handle_quiz(self, phone: str, chat_id: Optional[int], arg: Optional[str]) -> bool:
        """Handle /quiz command - start or interact with music quiz."""
        if not self.quiz_game:
            self.send(chat_id, phone, "Music quiz feature is not available.")
            return True
        
        partner = self.state.get_partner(phone)
        if not partner:
            self.send(chat_id, phone, "You need to be matched with someone first. Use /match.")
            return True
        
        # Check if there's an active quiz - if so, treat arg as a guess
        active_quiz = self.quiz_game.get_active_quiz(phone)
        if active_quiz and arg:
            return self.handle_quiz_guess(phone, chat_id, arg)
        
        # Start a new quiz
        can_start, reason = self.quiz_game.can_start_quiz(phone, partner)
        if not can_start:
            self.send(chat_id, phone, reason)
            return True
        
        success, message, quiz = self.quiz_game.start_quiz(phone, partner)
        if not success:
            self.send(chat_id, phone, message)
            return True
        
        # Send quiz to both users
        quiz_msg = self.quiz_game.get_quiz_message(quiz)
        partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
        
        self.send(chat_id, phone, quiz_msg)
        if partner_chat:
            self.send(partner_chat, partner, quiz_msg)
        
        return True
    
    def handle_quiz_guess(self, phone: str, chat_id: Optional[int], guess: str) -> bool:
        """Handle a quiz guess from a user."""
        if not self.quiz_game:
            return True
        
        is_correct, message, quiz = self.quiz_game.check_answer(phone, guess)
        
        if is_correct and quiz:
            # Notify both users
            partner = quiz.user_a if quiz.user_b == phone else quiz.user_b
            partner_chat = self.state.get_chat_id(partner) or self.switchboard.get_chat_id(partner)
            
            winner_msg = f"You got it! {message}"
            loser_msg = f"Your partner guessed it! The song was '{quiz.song_name}' by {quiz.artist}."
            
            self.send(chat_id, phone, winner_msg)
            if partner_chat:
                self.send(partner_chat, partner, loser_msg)
            
            # End the quiz
            self.quiz_game.end_quiz(phone)
        else:
            self.send(chat_id, phone, message)
        
        return True

    def run(self):
        """Run the main loop."""
        self.running = True
        signal.signal(signal.SIGINT, lambda s, f: setattr(self, 'running', False))
        signal.signal(signal.SIGTERM, lambda s, f: setattr(self, 'running', False))

        logger.info("SeriesSwarm running...")
        logger.info(f"Listening on topic: {self.kafka_topic}, group: {self.kafka_group_id}")

        try:
            poll_count = 0
            while self.running:
                message = self.consumer.consume(timeout=0.1)
                poll_count += 1
                if poll_count % 60 == 0:  # Log every 60 seconds
                    logger.info(f"Still polling... (poll #{poll_count})")
                if message:
                    logger.info(f"Received Kafka message: {message}")
                    self.process_message(message)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()
            logger.info("SeriesSwarm stopped")


def main():
    """Entry point."""
    try:
        app = SeriesSwarm()
        app.run()
    except Exception as e:
        logger.error(f"Failed to start: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
