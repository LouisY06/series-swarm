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
                .icon { 
                    width: 80px; 
                    height: 80px; 
                    margin: 0 auto 20px;
                }
                .icon svg { width: 100%; height: 100%; fill: #1DB954; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="icon">
                    <svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                        <path d="M12 0C5.4 0 0 5.4 0 12s5.4 12 12 12 12-5.4 12-12S18.66 0 12 0zm5.521 17.34c-.24.359-.66.48-1.021.24-2.82-1.74-6.36-2.101-10.561-1.141-.418.122-.779-.179-.899-.539-.12-.421.18-.78.54-.9 4.56-1.021 8.52-.6 11.64 1.32.42.18.479.659.301 1.02zm1.44-3.3c-.301.42-.841.6-1.262.3-3.239-1.98-8.159-2.58-11.939-1.38-.479.12-1.02-.12-1.14-.6-.12-.48.12-1.021.6-1.141C9.6 9.9 15 10.561 18.72 12.84c.361.181.54.78.241 1.2zm.12-3.36C15.24 8.4 8.82 8.16 5.16 9.301c-.6.179-1.2-.181-1.38-.721-.18-.601.18-1.2.72-1.381 4.26-1.26 11.28-1.02 15.721 1.621.539.3.719 1.02.419 1.56-.299.421-1.02.599-1.559.3z"/>
                    </svg>
                </div>
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

