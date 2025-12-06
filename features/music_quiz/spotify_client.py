"""Spotify OAuth and API client for music quiz feature."""

import os
import logging
from typing import Optional, Dict, List, Set, Tuple
from urllib.parse import urlencode

try:
    import spotipy
    from spotipy.oauth2 import SpotifyOAuth
    SPOTIPY_AVAILABLE = True
except ImportError:
    SPOTIPY_AVAILABLE = False

logger = logging.getLogger(__name__)


class SpotifyClient:
    """Handles Spotify OAuth and API interactions."""
    
    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None
    ):
        """Initialize Spotify client."""
        self.client_id = client_id or os.getenv('SPOTIFY_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('SPOTIFY_CLIENT_SECRET')
        self.redirect_uri = redirect_uri or os.getenv('SPOTIFY_REDIRECT_URI', 'http://localhost:8888/callback')
        
        self.scope = 'user-top-read'
        
        # Store user tokens: phone -> token_info
        self.user_tokens: Dict[str, Dict] = {}
        
        # Store pending auth states: state -> phone
        self.pending_auth: Dict[str, str] = {}
        
        if not SPOTIPY_AVAILABLE:
            logger.warning("spotipy not installed. Spotify features disabled.")
        elif not self.client_id or not self.client_secret:
            logger.warning("SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET not set.")
    
    def is_available(self) -> bool:
        """Check if Spotify client is properly configured."""
        return (
            SPOTIPY_AVAILABLE and 
            bool(self.client_id) and 
            bool(self.client_secret)
        )
    
    def get_auth_url(self, user_phone: str) -> Optional[str]:
        """Generate OAuth authorization URL for a user."""
        if not self.is_available():
            return None
        
        import secrets
        state = secrets.token_urlsafe(16)
        self.pending_auth[state] = user_phone
        
        params = {
            'client_id': self.client_id,
            'response_type': 'code',
            'redirect_uri': self.redirect_uri,
            'scope': self.scope,
            'state': state,
            'show_dialog': 'true'
        }
        
        auth_url = f"https://accounts.spotify.com/authorize?{urlencode(params)}"
        logger.info(f"Generated Spotify auth URL for {user_phone}")
        return auth_url
    
    def handle_callback(self, code: str, state: str) -> Optional[str]:
        """Handle OAuth callback and exchange code for token."""
        if not self.is_available():
            return None
        
        user_phone = self.pending_auth.pop(state, None)
        if not user_phone:
            logger.error(f"Unknown state in OAuth callback: {state}")
            return None
        
        try:
            oauth = SpotifyOAuth(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirect_uri=self.redirect_uri,
                scope=self.scope
            )
            
            token_info = oauth.get_access_token(code, as_dict=True)
            self.user_tokens[user_phone] = token_info
            logger.info(f"Stored Spotify token for {user_phone}")
            return user_phone
            
        except Exception as e:
            logger.error(f"Error exchanging Spotify code: {e}")
            return None
    
    def has_token(self, user_phone: str) -> bool:
        """Check if user has a valid Spotify token."""
        return user_phone in self.user_tokens
    
    def get_spotify_client(self, user_phone: str) -> Optional['spotipy.Spotify']:
        """Get authenticated Spotify client for a user."""
        if not self.is_available() or user_phone not in self.user_tokens:
            return None
        
        try:
            token_info = self.user_tokens[user_phone]
            
            # Check if token needs refresh
            oauth = SpotifyOAuth(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirect_uri=self.redirect_uri,
                scope=self.scope
            )
            
            # Check if token is expired and refresh if needed
            try:
                if oauth.is_token_expired(token_info):
                    refresh_token = token_info.get('refresh_token')
                    if refresh_token:
                        token_info = oauth.refresh_access_token(refresh_token)
                        self.user_tokens[user_phone] = token_info
                    else:
                        logger.error(f"No refresh token available for {user_phone}")
                        return None
            except Exception as refresh_error:
                logger.warning(f"Error checking/refreshing token for {user_phone}: {refresh_error}, trying anyway")
            
            access_token = token_info.get('access_token')
            if not access_token:
                logger.error(f"No access token for {user_phone}")
                return None
            
            return spotipy.Spotify(auth=access_token)
            
        except Exception as e:
            logger.error(f"Error getting Spotify client for {user_phone}: {e}")
            return None
    
    def get_top_tracks(
        self, 
        user_phone: str, 
        limit: int = 50, 
        time_range: str = 'medium_term'
    ) -> List[Dict]:
        """Get user's top tracks from Spotify."""
        sp = self.get_spotify_client(user_phone)
        if not sp:
            return []
        
        try:
            results = sp.current_user_top_tracks(limit=limit, time_range=time_range)
            tracks = []
            
            for track in results.get('items', []):
                tracks.append({
                    'id': track['id'],
                    'name': track['name'],
                    'artist': track['artists'][0]['name'] if track['artists'] else 'Unknown',
                    'album': track['album']['name'] if track.get('album') else 'Unknown',
                    'album_art': track['album']['images'][0]['url'] if track.get('album', {}).get('images') else None
                })
            
            logger.info(f"Fetched {len(tracks)} top tracks for {user_phone}")
            return tracks
            
        except Exception as e:
            logger.error(f"Error fetching top tracks for {user_phone}: {e}")
            return []
    
    def find_common_songs(
        self, 
        user_a: str, 
        user_b: str
    ) -> List[Dict]:
        """Find songs that both users have in their top tracks."""
        tracks_a = self.get_top_tracks(user_a)
        tracks_b = self.get_top_tracks(user_b)
        
        if not tracks_a or not tracks_b:
            return []
        
        # Create sets of (artist, name) tuples for comparison
        set_a: Set[Tuple[str, str]] = {
            (t['artist'].lower(), t['name'].lower()) 
            for t in tracks_a
        }
        set_b: Set[Tuple[str, str]] = {
            (t['artist'].lower(), t['name'].lower()) 
            for t in tracks_b
        }
        
        # Find intersection
        common_keys = set_a.intersection(set_b)
        
        # Get full track info for common songs
        common_songs = []
        for track in tracks_a:
            key = (track['artist'].lower(), track['name'].lower())
            if key in common_keys:
                common_songs.append(track)
        
        logger.info(f"Found {len(common_songs)} common songs between {user_a} and {user_b}")
        return common_songs

