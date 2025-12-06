"""Genius API client for fetching song lyrics."""

import os
import re
import random
import logging
from typing import Optional, Dict

try:
    import lyricsgenius
    GENIUS_AVAILABLE = True
except ImportError:
    GENIUS_AVAILABLE = False

logger = logging.getLogger(__name__)


class GeniusClient:
    """Handles Genius API interactions for lyrics fetching."""
    
    def __init__(self, access_token: Optional[str] = None):
        """Initialize Genius client."""
        self.access_token = access_token or os.getenv('GENIUS_ACCESS_TOKEN')
        self.genius = None
        
        if not GENIUS_AVAILABLE:
            logger.warning("lyricsgenius not installed. Lyrics features disabled.")
        elif not self.access_token:
            logger.warning("GENIUS_ACCESS_TOKEN not set.")
        else:
            try:
                self.genius = lyricsgenius.Genius(
                    self.access_token,
                    verbose=False,
                    remove_section_headers=True
                )
                self.genius.timeout = 10
                logger.info("Genius client initialized")
            except Exception as e:
                logger.error(f"Error initializing Genius client: {e}")
    
    def is_available(self) -> bool:
        """Check if Genius client is properly configured."""
        return self.genius is not None
    
    def get_lyrics(self, artist: str, song_name: str) -> Optional[str]:
        """Fetch full lyrics for a song."""
        if not self.is_available():
            return None
        
        try:
            song = self.genius.search_song(song_name, artist)
            if song and song.lyrics:
                return song.lyrics
            return None
        except Exception as e:
            logger.error(f"Error fetching lyrics for {artist} - {song_name}: {e}")
            return None
    
    def clean_lyrics(self, lyrics: str) -> list:
        """Clean lyrics by removing metadata and empty lines."""
        lines = lyrics.split('\n')
        clean_lines = []
        
        for line in lines:
            # Skip section headers like [Chorus], [Verse 1], etc.
            if re.match(r'^\[.*\]$', line.strip()):
                continue
            # Skip empty lines
            if not line.strip():
                continue
            # Skip lines that are just the song title or "Lyrics"
            if 'Lyrics' in line and len(line) < 50:
                continue
            # Skip contributor/embed text
            if 'Embed' in line or 'Contributors' in line:
                continue
            
            clean_lines.append(line.strip())
        
        return clean_lines
    
    def get_lyric_snippet(
        self, 
        artist: str, 
        song_name: str, 
        num_lines: int = 4
    ) -> Optional[Dict]:
        """Get a random snippet of lyrics for quiz purposes."""
        lyrics = self.get_lyrics(artist, song_name)
        if not lyrics:
            return None
        
        clean_lines = self.clean_lyrics(lyrics)
        
        if len(clean_lines) < num_lines:
            return None
        
        # Pick a random starting point
        max_start = len(clean_lines) - num_lines
        if max_start <= 0:
            start_index = 0
        else:
            start_index = random.randint(0, max_start)
        
        snippet_lines = clean_lines[start_index:start_index + num_lines]
        snippet = '\n'.join(snippet_lines)
        
        logger.info(f"Generated lyric snippet for {artist} - {song_name}")
        
        return {
            'artist': artist,
            'song_name': song_name,
            'snippet': snippet,
            'line_count': num_lines
        }
    
    def mask_song_title_in_snippet(self, snippet: str, song_name: str) -> str:
        """Replace any occurrence of the song title in the snippet with blanks."""
        # Case-insensitive replacement
        pattern = re.compile(re.escape(song_name), re.IGNORECASE)
        masked = pattern.sub('_____', snippet)
        return masked

