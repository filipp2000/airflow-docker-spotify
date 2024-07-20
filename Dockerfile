# Start with the official Airflow image
FROM apache/airflow:2.9.3
USER airflow
# Install additional Python packages
RUN pip install --no-cache-dir spotipy flask pandas sqlalchemy pytz
