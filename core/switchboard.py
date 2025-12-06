"""Switchboard state management for matchmaking system."""

import logging
from typing import Dict, Set, List, Optional, Tuple

logger = logging.getLogger(__name__)


class Switchboard:
    """Manages user pairing, message relay, and contact reveal logic."""

    def __init__(self):
        """Initialize switchboard with empty state."""
        self.waiting_queue: List[str] = []  # Phone numbers waiting for a match
        self.active_pairs: Dict[str, str] = {}  # Bidirectional: user -> partner
        self.reveal_requests: Set[str] = set()  # Users who requested reveal
        self.user_profiles: Dict[str, Dict[str, str]] = {}  # phone -> {name, email, phone, etc.}
        self.user_chat_ids: Dict[str, int] = {}  # phone -> chat_id mapping
        logger.info("Switchboard initialized")

    def find_match(self, user_phone: str) -> Optional[str]:
        """
        Pair a user with someone from the waiting queue.

        Args:
            user_phone: Phone number of user looking for a match

        Returns:
            Partner phone number if matched, None if added to queue
        """
        if not self.waiting_queue:
            # No one waiting, add to queue
            if user_phone not in self.waiting_queue:
                self.waiting_queue.append(user_phone)
                logger.info(f"User {user_phone} added to waiting queue (queue size: {len(self.waiting_queue)})")
            return None

        # Someone is waiting! Pair them up
        partner_phone = self.waiting_queue.pop(0)

        # Create bidirectional mapping
        self.active_pairs[user_phone] = partner_phone
        self.active_pairs[partner_phone] = user_phone

        logger.info(f"Matched {user_phone} with {partner_phone}")
        return partner_phone

    def end_chat(self, user_phone: str) -> Optional[str]:
        """
        Disconnect the current pair and requeue the user who skipped.

        Args:
            user_phone: Phone number of user disconnecting

        Returns:
            Partner phone number if there was a pair, None otherwise
        """
        if user_phone not in self.active_pairs:
            logger.debug(f"User {user_phone} not in active pair")
            return None

        partner = self.active_pairs[user_phone]

        # Clear state
        del self.active_pairs[user_phone]
        del self.active_pairs[partner]

        # Clear reveal requests
        self.reveal_requests.discard(user_phone)
        self.reveal_requests.discard(partner)

        logger.info(f"Disconnected pair: {user_phone} and {partner}")

        # Requeue the user who skipped
        if user_phone not in self.waiting_queue:
            self.waiting_queue.append(user_phone)
            logger.info(f"User {user_phone} requeued")

        return partner

    def handle_reveal(self, user_phone: str) -> Tuple[bool, Optional[str]]:
        """
        Handle the reveal request. Returns True if both users agreed.

        Args:
            user_phone: Phone number of user requesting reveal

        Returns:
            Tuple of (both_agreed: bool, partner_phone: Optional[str])
        """
        if user_phone not in self.active_pairs:
            logger.warning(f"User {user_phone} not in active pair, cannot reveal")
            return False, None

        partner = self.active_pairs[user_phone]

        # Add user to reveal requests
        self.reveal_requests.add(user_phone)

        # Check if both agreed
        if partner in self.reveal_requests:
            # Both agreed! Clear reveal requests
            self.reveal_requests.discard(user_phone)
            self.reveal_requests.discard(partner)
            logger.info(f"Both users agreed to reveal: {user_phone} and {partner}")
            return True, partner
        else:
            logger.info(f"User {user_phone} requested reveal, waiting for {partner}")
            return False, partner

    def get_partner(self, user_phone: str) -> Optional[str]:
        """
        Get the partner of a user if they are in an active pair.

        Args:
            user_phone: Phone number of user

        Returns:
            Partner phone number or None
        """
        return self.active_pairs.get(user_phone)

    def is_paired(self, user_phone: str) -> bool:
        """Check if user is currently paired."""
        return user_phone in self.active_pairs

    def is_waiting(self, user_phone: str) -> bool:
        """Check if user is in waiting queue."""
        return user_phone in self.waiting_queue

    def store_user_profile(self, phone: str, profile_data: Dict[str, str]):
        """
        Store user profile information.

        Args:
            phone: User's phone number
            profile_data: Dictionary with name, email, phone, etc.
        """
        if phone not in self.user_profiles:
            self.user_profiles[phone] = {}

        self.user_profiles[phone].update(profile_data)
        logger.info(f"Updated profile for {phone}: {list(profile_data.keys())}")

    def get_user_profile(self, phone: str) -> Dict[str, str]:
        """
        Get user profile information.

        Args:
            phone: User's phone number

        Returns:
            Dictionary with user profile data
        """
        profile = self.user_profiles.get(phone, {})
        # Ensure phone is always in profile
        if 'phone' not in profile:
            profile['phone'] = phone
        return profile

    def store_chat_id(self, phone: str, chat_id: int):
        """Store chat_id for a user."""
        self.user_chat_ids[phone] = chat_id
        logger.debug(f"Stored chat_id {chat_id} for {phone}")

    def get_chat_id(self, phone: str) -> Optional[int]:
        """Get chat_id for a user."""
        return self.user_chat_ids.get(phone)

    def get_stats(self) -> Dict[str, int]:
        """Get current switchboard statistics."""
        return {
            'waiting': len(self.waiting_queue),
            'active_pairs': len(self.active_pairs) // 2,
            'reveal_requests': len(self.reveal_requests),
            'total_users': len(self.user_profiles)
        }

