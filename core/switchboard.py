"""Switchboard for managing matchmaking state."""

import logging
from typing import Optional, Dict, List, Set, Tuple

logger = logging.getLogger(__name__)


def _is_fake_test_data(value: str) -> bool:
    """Check if a value looks like fake/test data."""
    if not value:
        return False
    
    value_lower = value.lower().strip()
    
    # Common fake/test patterns
    fake_patterns = [
        'john appleseed',
        'john apple',
        'appleseed',
        'example.com',
        'test@',
        'fake@',
        'dummy',
        'test user',
        'sample',
    ]
    
    for pattern in fake_patterns:
        if pattern in value_lower:
            return True
    
    return False


class Switchboard:
    """Manages user pairing, message relay, and contact reveal."""

    def __init__(self):
        """Initialize switchboard state."""
        self.waiting_queue: List[str] = []
        self.active_pairs: Dict[str, str] = {}  # user -> partner (bidirectional)
        self.reveal_requests: Set[str] = set()
        self.user_profiles: Dict[str, Dict[str, str]] = {}
        self.user_chat_ids: Dict[str, int] = {}
        logger.info("Switchboard initialized")

    def find_match(self, user_phone: str) -> Optional[str]:
        """
        Find a match for the user.
        Returns partner phone if matched, None if added to queue.
        """
        # Don't match if already paired
        if user_phone in self.active_pairs:
            return self.active_pairs[user_phone]

        # Remove from queue if already there
        if user_phone in self.waiting_queue:
            self.waiting_queue.remove(user_phone)

        # Check if anyone is waiting
        if self.waiting_queue:
            partner = self.waiting_queue.pop(0)
            # Create bidirectional mapping
            self.active_pairs[user_phone] = partner
            self.active_pairs[partner] = user_phone
            logger.info(f"Matched {user_phone} with {partner}")
            return partner
        else:
            # Add to queue
            self.waiting_queue.append(user_phone)
            logger.info(f"User {user_phone} added to queue (size: {len(self.waiting_queue)})")
            return None

    def end_chat(self, user_phone: str) -> Optional[str]:
        """
        End the current chat and return the partner's phone.
        """
        if user_phone not in self.active_pairs:
            return None

        partner = self.active_pairs[user_phone]

        # Remove pair
        del self.active_pairs[user_phone]
        del self.active_pairs[partner]

        # Clear reveal requests
        self.reveal_requests.discard(user_phone)
        self.reveal_requests.discard(partner)

        logger.info(f"Ended chat between {user_phone} and {partner}")
        return partner

    def handle_reveal(self, user_phone: str) -> Tuple[bool, Optional[str]]:
        """
        Handle reveal request. Returns (both_agreed, partner_phone).
        """
        if user_phone not in self.active_pairs:
            return False, None

        partner = self.active_pairs[user_phone]
        self.reveal_requests.add(user_phone)

        if partner in self.reveal_requests:
            # Both agreed
            self.reveal_requests.discard(user_phone)
            self.reveal_requests.discard(partner)
            logger.info(f"Both agreed to reveal: {user_phone} and {partner}")
            return True, partner
        else:
            logger.info(f"{user_phone} requested reveal, waiting for {partner}")
            return False, partner

    def get_partner(self, user_phone: str) -> Optional[str]:
        """Get the partner of a user."""
        return self.active_pairs.get(user_phone)

    def is_paired(self, user_phone: str) -> bool:
        """Check if user is paired."""
        return user_phone in self.active_pairs

    def is_waiting(self, user_phone: str) -> bool:
        """Check if user is waiting."""
        return user_phone in self.waiting_queue

    def store_chat_id(self, phone: str, chat_id: int):
        """Store chat ID for a user."""
        self.user_chat_ids[phone] = chat_id

    def get_chat_id(self, phone: str) -> Optional[int]:
        """Get chat ID for a user."""
        return self.user_chat_ids.get(phone)

    def store_user_profile(self, phone: str, data: Dict[str, str]):
        """Store/update user profile. Rejects fake/test data."""
        if phone not in self.user_profiles:
            self.user_profiles[phone] = {'phone': phone}
        
        # Filter out fake/test data
        filtered_data = {}
        for key, value in data.items():
            if value and not _is_fake_test_data(str(value)):
                filtered_data[key] = value
            elif value:
                logger.warning(f"Rejected fake/test data for {phone}: {key}={value[:50]}")
        
        self.user_profiles[phone].update(filtered_data)
        logger.info(f"Updated profile for {phone}")

    def get_user_profile(self, phone: str) -> Dict[str, str]:
        """Get user profile. Returns only real data, filters out fake/test data."""
        profile = self.user_profiles.get(phone, {})
        if 'phone' not in profile:
            profile['phone'] = phone
        
        # Filter out any fake/test data that might have been stored
        filtered_profile = {'phone': profile.get('phone', phone)}
        for key, value in profile.items():
            if key == 'phone':
                continue
            if value and not _is_fake_test_data(str(value)):
                filtered_profile[key] = value
        
        return filtered_profile

