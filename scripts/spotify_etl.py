import pandas as pd
import sqlalchemy
from datetime import datetime, timedelta
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import logging
import pytz
from dotenv import load_dotenv
import os

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Specify the path to the .env file
env_path = "/opt/airflow/scripts/.env"
load_dotenv(env_path)


# Get Spotify credentials from the .env file
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
SCOPE = os.getenv('SCOPE')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')

def authenticate_spotify() -> spotipy.Spotify:
    """
    Authenticates with the Spotify API using a refresh token to obtain a new access token.
    """
    
    try:
        sp_oauth = SpotifyOAuth(client_id=CLIENT_ID,
                                client_secret=CLIENT_SECRET,
                                redirect_uri=REDIRECT_URI,
                                scope=SCOPE)
        
        # Refresh the access token using the refresh token
        token_info = sp_oauth.refresh_access_token(REFRESH_TOKEN)
        
        # Create and return an authenticated Spotify client
        sp = spotipy.Spotify(auth=token_info['access_token'])
        return sp
    except Exception as e:
        logger.error("Failed to authenticate with Spotify: %s", e)
        raise

def check_if_valid_data(df: pd.DataFrame) -> bool:
    """
    Function to validate the data
    """
    
    # Check if dataframe is empty
    if df.empty:
        logger.warning("No songs retrieved. Finishing execution")
        return False
    
    # Check for nulls
    if df.isnull().values.any():
        logger.error("Data validation failed: Null values found")
        return False

    # Check for unique 'played_at' values
    if not df['played_at'].is_unique:
        logger.error("Data validation failed: Duplicate 'played_at' values found")
        return False
    
    
    # Convert played_at to datetime with timezone awareness
    timestamps = pd.to_datetime(df["played_at"], utc=True)

    # Check that all timestamps are within the last 24 hours
    now = datetime.now(pytz.UTC)
    yesterday = now - timedelta(days=1)
    
    # Check that all timestamps are within the last 24 hours
    if not timestamps.between(yesterday, now).all():
        logger.error("Timestamp validation failed: not all timestamps are within the last 24 hours")
        return False

    logger.info("Data validation passed")

    return True

def extract_spotify_data(sp: spotipy.Spotify) -> pd.DataFrame:
    """
    Extracts data from Spotify API.
    """
    
    # Convert time to Unix timestamp in milliseconds      
    # We need the 'yesterday' timestamp because we want to run this feed daily 
    # and every day we want to see what songs we played in the last day(24hours)
    now = datetime.now(pytz.UTC)
    yesterday = now - timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    try:
        # Get recently played tracks after the specified timestamp
        results = sp.current_user_recently_played(limit=50, after=yesterday_unix_timestamp)
        
        song_names = []
        artist_names = []
        album_names = []
        genres = []
        durations = []
        played_at_list = []
        
        # Extract relevant data
        for item in results['items']:
            song_names.append(item['track']['name'])
            artist_names.append(item['track']['album']['artists'][0]['name'])
            album_names.append(item['track']['album']['name'])
            # Get artist details for genres
            artist_data = sp.artist(item['track']['artists'][0]['id'])
            genres.append(', '.join(artist_data['genres']))
            durations.append(item['track']['duration_ms'])
            played_at_list.append(item['played_at'])

        # Prepare a dictionary in order to turn it into a pandas dataframe below
        song_dict = {
            "song_name": song_names,
            "artist_name": artist_names,
            "album_name": album_names,
            "genre": genres,
            "duration_ms": durations,
            "played_at": played_at_list
        }
        
        song_df = pd.DataFrame(song_dict)
        logger.info("Extract part of the ETL process completed. DataFrame created")
        return song_df
    
    except Exception as e:
        logger.error("Failed to extract data from Spotify: %s", e)
        raise

def load_data_to_db(df: pd.DataFrame, db_location: str):
    """
    Loads data into the SQLite database.
    """
    try:
        engine = sqlalchemy.create_engine(f'sqlite:///{db_location}')
        logger.info(f"Database engine created in {db_location}")
        
        # Insert data into the table
        df.to_sql('songs_table', engine, index=False, if_exists='append')
        logger.info("Data inserted successfully")
    except Exception as e:
        logger.exception("Error inserting data: %s", e)
        raise

def run_spotify_etl():
    """Runs the Spotify ETL process."""
    try:
        # Authentication part
        sp = authenticate_spotify()
        logger.info("Authentication completed. Starting Spotify ETL process.")
        
        # Extract part
        song_df = extract_spotify_data(sp)
        
        if check_if_valid_data(song_df):
            logger.info("Data validation completed. Proceed to Load stage")
            # Ensure this path is accessible within your Docker container
            db_location = '/opt/airflow/scripts/my_played_songs.db'
            load_data_to_db(song_df, db_location)
            logger.info("Spotify ETL process completed successfully.")
        else:
            logger.error("Invalid data. ETL process stopped")
    except Exception as e:
        logger.error("ETL process failed: %s", e)

# if __name__ == "__main__":
#     run_spotify_etl()
