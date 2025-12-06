"""Quiz game logic and state management."""

import time
import random
import logging
from typing import Optional, Dict, Tuple, List
from dataclasses import dataclass, field
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


@dataclass
class QuizState:
    """State for an active quiz between two users."""
    user_a: str
    user_b: str
    song_name: str
    artist: str
    snippet: str
    album_art: Optional[str] = None
    started_at: float = field(default_factory=time.time)
    winner: Optional[str] = None
    guesses: Dict[str, List[str]] = field(default_factory=dict)
    
    def is_active(self) -> bool:
        """Check if quiz is still active (no winner yet)."""
        return self.winner is None
    
    def time_elapsed(self) -> float:
        """Get seconds since quiz started."""
        return time.time() - self.started_at


class QuizGame:
    """Manages music quiz games between matched users."""
    
    def __init__(self, spotify_client, genius_client):
        """Initialize quiz game manager."""
        self.spotify = spotify_client
        self.genius = genius_client
        
        # Active quizzes: (user_a, user_b) tuple -> QuizState
        self.active_quizzes: Dict[Tuple[str, str], QuizState] = {}
        
        # User to quiz mapping for quick lookup
        self.user_quiz_map: Dict[str, Tuple[str, str]] = {}
    
    def _get_pair_key(self, user_a: str, user_b: str) -> Tuple[str, str]:
        """Get canonical pair key (sorted)."""
        return tuple(sorted([user_a, user_b]))
    
    def has_active_quiz(self, user: str) -> bool:
        """Check if user is in an active quiz."""
        if user not in self.user_quiz_map:
            return False
        pair_key = self.user_quiz_map[user]
        quiz = self.active_quizzes.get(pair_key)
        return quiz is not None and quiz.is_active()
    
    def get_active_quiz(self, user: str) -> Optional[QuizState]:
        """Get active quiz for a user."""
        if user not in self.user_quiz_map:
            return None
        pair_key = self.user_quiz_map[user]
        quiz = self.active_quizzes.get(pair_key)
        if quiz and quiz.is_active():
            return quiz
        return None
    
    def can_start_quiz(self, user_a: str, user_b: str) -> Tuple[bool, str]:
        """Check if quiz can be started between two users."""
        # Check if both have Spotify linked
        if not self.spotify.has_token(user_a):
            return False, f"You need to link Spotify first. Use /spotify"
        if not self.spotify.has_token(user_b):
            return False, "Your partner hasn't linked Spotify yet."
        
        # Check for existing active quiz
        if self.has_active_quiz(user_a) or self.has_active_quiz(user_b):
            return False, "There's already an active quiz. Answer it first!"
        
        return True, ""
    
    def start_quiz(self, user_a: str, user_b: str) -> Tuple[bool, str, Optional[QuizState]]:
        """Start a new quiz between two matched users."""
        can_start, reason = self.can_start_quiz(user_a, user_b)
        if not can_start:
            return False, reason, None
        
        # Find common songs
        common_songs = self.spotify.find_common_songs(user_a, user_b)
        
        if not common_songs:
            return False, "You don't have any songs in common in your top 50. Try listening to more similar music!", None
        
        # Pick a random song
        song = random.choice(common_songs)
        
        # Get lyrics snippet
        snippet_data = self.genius.get_lyric_snippet(song['artist'], song['name'])
        
        if not snippet_data:
            # Try another song if lyrics not found
            for fallback_song in common_songs:
                if fallback_song != song:
                    snippet_data = self.genius.get_lyric_snippet(
                        fallback_song['artist'], 
                        fallback_song['name']
                    )
                    if snippet_data:
                        song = fallback_song
                        break
        
        if not snippet_data:
            return False, "Couldn't find lyrics for your common songs. Try again later!", None
        
        # Mask song title in snippet
        masked_snippet = self.genius.mask_song_title_in_snippet(
            snippet_data['snippet'], 
            song['name']
        )
        
        # Create quiz state
        pair_key = self._get_pair_key(user_a, user_b)
        quiz = QuizState(
            user_a=user_a,
            user_b=user_b,
            song_name=song['name'],
            artist=song['artist'],
            snippet=masked_snippet,
            album_art=song.get('album_art')
        )
        
        self.active_quizzes[pair_key] = quiz
        self.user_quiz_map[user_a] = pair_key
        self.user_quiz_map[user_b] = pair_key
        
        logger.info(f"Started quiz between {user_a} and {user_b}: {song['artist']} - {song['name']}")
        
        return True, "", quiz
    
    def _similarity(self, a: str, b: str) -> float:
        """Calculate similarity ratio between two strings."""
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()
    
    def check_answer(self, user: str, guess: str) -> Tuple[bool, str, Optional[QuizState]]:
        """Check if user's guess is correct. Returns (is_correct, message, quiz)."""
        quiz = self.get_active_quiz(user)
        if not quiz:
            return False, "No active quiz found.", None
        
        guess_clean = guess.strip().lower()
        
        # Check for give up / idk
        if guess_clean in ('idk', "i don't know", "i dont know", "give up", "giveup", "skip"):
            quiz.winner = "nobody"  # Mark as ended without winner
            logger.info(f"Quiz given up by {user}, answer was '{quiz.song_name}'")
            return True, f"The song was '{quiz.song_name}' by {quiz.artist}. This song is in BOTH of your top 50!", quiz
        
        # Record the guess
        if user not in quiz.guesses:
            quiz.guesses[user] = []
        quiz.guesses[user].append(guess)
        
        song_clean = quiz.song_name.lower()
        
        # Check for exact match or high similarity
        similarity = self._similarity(guess_clean, song_clean)
        
        # Also check if guess is contained in song name or vice versa
        contains_match = guess_clean in song_clean or song_clean in guess_clean
        
        is_correct = similarity > 0.7 or contains_match
        
        if is_correct:
            quiz.winner = user
            logger.info(f"Quiz won by {user}: guessed '{guess}' for '{quiz.song_name}'")
            return True, f"Correct! The song is '{quiz.song_name}' by {quiz.artist}. This song is in BOTH of your top 50!", quiz
        else:
            return False, "Wrong! Try again. (Type 'idk' to give up)", quiz
    
    def end_quiz(self, user: str) -> bool:
        """End an active quiz for a user."""
        if user not in self.user_quiz_map:
            return False
        
        pair_key = self.user_quiz_map[user]
        quiz = self.active_quizzes.get(pair_key)
        
        if quiz:
            # Clean up
            self.active_quizzes.pop(pair_key, None)
            self.user_quiz_map.pop(quiz.user_a, None)
            self.user_quiz_map.pop(quiz.user_b, None)
            logger.info(f"Ended quiz between {quiz.user_a} and {quiz.user_b}")
            return True
        
        return False
    
    def get_quiz_message(self, quiz: QuizState) -> str:
        """Format quiz question for sending to users."""
        return f"""GUESS THE SONG!

Both of you listen to this song. First to reply with the song title wins!

---
{quiz.snippet}
---

Reply with your guess!"""
    
    def get_reveal_message(self, quiz: QuizState) -> str:
        """Format reveal message after quiz ends."""
        return f"""The song was: {quiz.song_name}
Artist: {quiz.artist}

This song is in BOTH of your Top 50 on Spotify!"""

