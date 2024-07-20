import spotipy
from spotipy.oauth2 import SpotifyOAuth
import webbrowser
import logging
from dotenv import load_dotenv, set_key
import os
import http.server
import socketserver
import threading

# Specify the path to the .env file
env_path = "C:\\Users\\filip\\OneDrive\\Desktop\\Data Engineering Projects\\airflow-docker\\scripts\\.env"
load_dotenv(env_path)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)  # creates a logger object specific to the module name

# Get the Spotify credentials from the .env file
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
SCOPE = os.getenv('SCOPE')

# HTTP handler for the callback
class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if '/callback' in self.path:
            self.server.auth_code = self.path.split('code=')[1]
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"Authorization successful. You can close this window.")
            self.server.stop_event.set()
            threading.Thread(target=self.shutdown_server).start()

    def shutdown_server(self):
        self.server.shutdown()

def get_refresh_token():
    sp_oauth = SpotifyOAuth(client_id=CLIENT_ID,
                            client_secret=CLIENT_SECRET,
                            redirect_uri=REDIRECT_URI,
                            scope=SCOPE)
    
    # Create an authorization URL
    auth_url = sp_oauth.get_authorize_url()
    webbrowser.open(auth_url)
    logger.info("Opening browser for Spotify authorization...")

    # Start the HTTP server
    with socketserver.TCPServer(("localhost", 3000), Handler) as httpd:
        httpd.auth_code = None  # Initialize the auth_code attribute
        httpd.stop_event = threading.Event()  # Initialize the event attribute
        logger.info("Serving at port 3000")
        server_thread = threading.Thread(target=httpd.serve_forever)
        server_thread.start()

        # Wait until the event is set
        httpd.stop_event.wait()

        httpd.shutdown()
        server_thread.join()

    # Exchange the authorization code for tokens
    token_info = sp_oauth.get_access_token(httpd.auth_code)
    
    # Save the refresh token to a .env file
    set_key(env_path, "ACCESS_TOKEN", token_info['access_token'])
    set_key(env_path, "REFRESH_TOKEN", token_info['refresh_token'])

    return token_info['refresh_token']

def main():
    try:
        refresh_token = get_refresh_token()
        logger.info(f"Refresh token obtained: {refresh_token}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        logger.info("Exiting script")

if __name__ == "__main__":
    main()
