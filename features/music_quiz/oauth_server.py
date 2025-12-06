"""OAuth callback server for Spotify authentication."""

import os
import logging
import threading
from typing import Optional, Callable

try:
    from flask import Flask, request, redirect
    FLASK_AVAILABLE = True
except ImportError:
    FLASK_AVAILABLE = False

logger = logging.getLogger(__name__)


class OAuthCallbackServer:
    """Simple Flask server to handle OAuth callbacks."""
    
    def __init__(
        self, 
        spotify_client,
        on_auth_complete: Optional[Callable[[str, bool], None]] = None,
        host: str = '127.0.0.1',
        port: int = 8888
    ):
        """
        Initialize OAuth callback server.
        
        Args:
            spotify_client: SpotifyClient instance to handle token exchange
            on_auth_complete: Callback function(user_phone, success) called when auth completes
            host: Host to bind to
            port: Port to listen on
        """
        self.spotify_client = spotify_client
        self.on_auth_complete = on_auth_complete
        self.host = host
        self.port = port
        self.app = None
        self.server_thread = None
        
        if not FLASK_AVAILABLE:
            logger.warning("Flask not installed. OAuth callback server disabled.")
            return
        
        self._setup_app()
    
    def _setup_app(self):
        """Set up Flask app with routes."""
        self.app = Flask(__name__)
        self.app.logger.setLevel(logging.WARNING)  # Reduce Flask logging noise
        
        @self.app.route('/callback')
        def callback():
            """Handle Spotify OAuth callback."""
            code = request.args.get('code')
            state = request.args.get('state')
            error = request.args.get('error')
            
            if error:
                logger.error(f"Spotify OAuth error: {error}")
                return self._error_page(f"Spotify authorization failed: {error}")
            
            if not code or not state:
                logger.error("Missing code or state in callback")
                return self._error_page("Invalid callback - missing parameters")
            
            # Exchange code for token
            user_phone = self.spotify_client.handle_callback(code, state)
            
            if user_phone:
                logger.info(f"Spotify auth completed for {user_phone}")
                if self.on_auth_complete:
                    self.on_auth_complete(user_phone, True)
                return self._success_page()
            else:
                logger.error("Failed to complete Spotify auth")
                if self.on_auth_complete:
                    self.on_auth_complete(None, False)
                return self._error_page("Failed to complete authorization. Please try /spotify again.")
        
        @self.app.route('/health')
        def health():
            """Health check endpoint."""
            return "OK", 200
    
    def _success_page(self) -> str:
        """Return success HTML page."""
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Spotify Connected!</title>
            <style>
                body {
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    margin: 0;
                    background: linear-gradient(135deg, #1DB954 0%, #191414 100%);
                    color: white;
                }
                .container {
                    text-align: center;
                    padding: 40px;
                    background: rgba(0,0,0,0.3);
                    border-radius: 20px;
                }
                h1 { font-size: 2.5em; margin-bottom: 10px; }
                p { font-size: 1.2em; opacity: 0.9; }
                .icon { font-size: 4em; margin-bottom: 20px; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="icon">🎵</div>
                <h1>Spotify Connected!</h1>
                <p>You can close this window and return to the chat.</p>
                <p>Use <strong>/quiz</strong> to start a music game with your partner!</p>
            </div>
        </body>
        </html>
        """
    
    def _error_page(self, message: str) -> str:
        """Return error HTML page."""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Connection Failed</title>
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    margin: 0;
                    background: linear-gradient(135deg, #e74c3c 0%, #2c3e50 100%);
                    color: white;
                }}
                .container {{
                    text-align: center;
                    padding: 40px;
                    background: rgba(0,0,0,0.3);
                    border-radius: 20px;
                }}
                h1 {{ font-size: 2em; margin-bottom: 10px; }}
                p {{ font-size: 1.1em; opacity: 0.9; }}
                .icon {{ font-size: 4em; margin-bottom: 20px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="icon">❌</div>
                <h1>Connection Failed</h1>
                <p>{message}</p>
                <p>Try sending <strong>/spotify</strong> again in the chat.</p>
            </div>
        </body>
        </html>
        """
    
    def start(self, threaded: bool = True):
        """Start the OAuth callback server."""
        if not FLASK_AVAILABLE or not self.app:
            logger.warning("Cannot start OAuth server - Flask not available")
            return False
        
        def run_server():
            # Disable Flask's default logging
            import logging as flask_logging
            flask_logging.getLogger('werkzeug').setLevel(flask_logging.WARNING)
            
            try:
                self.app.run(
                    host=self.host, 
                    port=self.port, 
                    debug=False, 
                    use_reloader=False,
                    threaded=True
                )
            except Exception as e:
                logger.error(f"OAuth server error: {e}")
        
        if threaded:
            self.server_thread = threading.Thread(target=run_server, daemon=True)
            self.server_thread.start()
            logger.info(f"OAuth callback server started on http://{self.host}:{self.port}")
            return True
        else:
            run_server()
            return True
    
    def is_available(self) -> bool:
        """Check if server can be started."""
        return FLASK_AVAILABLE and self.app is not None

