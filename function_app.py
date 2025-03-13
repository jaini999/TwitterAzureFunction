# File: function_app.py
from azure.storage.blob import BlobServiceClient
import os
import csv
import logging
import tempfile
import azure.functions as func
import tweepy
import datetime
from pathlib import Path



def check_env_variables():
    logging.info(f"TWITTER_CONSUMER_KEY: {os.environ.get('TWITTER_CONSUMER_KEY')}")
    logging.info(f"TWITTER_CONSUMER_SECRET: {os.environ.get('TWITTER_CONSUMER_SECRET')}")
    logging.info(f"TWITTER_ACCESS_TOKEN: {os.environ.get('TWITTER_ACCESS_TOKEN')}")
    logging.info(f"TWITTER_ACCESS_TOKEN_SECRET: {os.environ.get('TWITTER_ACCESS_TOKEN_SECRET')}")

check_env_variables()

app = func.FunctionApp()

@app.function_name(name="HourlyTrigger")
@app.schedule(schedule="0 0 * * * *", arg_name="mytimer", run_on_startup=False) # Runs at the top of every hour
def tweet_scheduler(mytimer: func.TimerRequest) -> None:
    logging.info('Twitter function triggered at %s', datetime.datetime.now().isoformat())

    try:
        # Load Twitter API credentials from environment variables
        consumer_key = os.environ.get('TWITTER_CONSUMER_KEY')
        consumer_secret = os.environ.get('TWITTER_CONSUMER_SECRET')
        access_token = os.environ.get('TWITTER_ACCESS_TOKEN')
        access_token_secret = os.environ.get('TWITTER_ACCESS_TOKEN_SECRET')

        # Initialize Twitter API client
        client = tweepy.Client(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret
        )

        # Get the tweet content
        tweet_content = get_next_tweet()
        
        if tweet_content:
            # Post the tweet
            response = client.create_tweet(text=tweet_content)
            logging.info(f"Tweet posted successfully! Tweet ID: {response.data['id']}")
        else:
            logging.warning("No tweet content available to post.")
            
    except Exception as e:
        logging.error(f"Error posting tweet: {str(e)}")

# Azure Storage details
AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AzureWebJobsStorage")
CONTAINER_NAME = "tweets-container"  # Replace with your actual container name
BLOB_NAME = "tweets.csv"

def download_tweets_file():
    """Download tweets.csv from Azure Blob Storage to a temporary location."""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

        # Create a temp file
        temp_file_path = os.path.join(tempfile.gettempdir(), "tweets.csv")

        # Download the blob contents to a file
        with open(temp_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())

        logging.info("Downloaded tweets.csv successfully")
        return temp_file_path
    except Exception as e:
        logging.error(f"Failed to download tweets.csv: {e}")
        return None

def get_next_tweet():
    """Get the next tweet from Azure Blob Storage and update it."""
    tweets_file = download_tweets_file()
    if not tweets_file:
        return None  # Exit if download failed

    temp_file = tweets_file + ".tmp"  # Temporary file for updates
    tweet_to_post = None

    try:
        with open(tweets_file, 'r', newline='', encoding='utf-8') as input_file, open(temp_file, 'w', newline='', encoding='utf-8') as output_file:
            reader = csv.reader(input_file)
            writer = csv.writer(output_file)

            header = next(reader)
            writer.writerow(header)

            for row in reader:
                if len(row) >= 2:
                    tweet, posted = row[0], row[1].lower() == 'true'

                    if tweet_to_post is None and not posted:
                        tweet_to_post = tweet
                        writer.writerow([tweet, 'true'])
                    else:
                        writer.writerow(row)

        # Upload the updated file back to Azure Blob Storage
        upload_tweets_file(temp_file)

        return tweet_to_post

    except Exception as e:
        logging.error(f"Error processing tweets file: {e}")
        return None

def upload_tweets_file(temp_file):
    """Upload the updated tweets file back to Azure Blob Storage."""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)

        with open(temp_file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        logging.info("Uploaded updated tweets.csv to Azure Blob Storage")

    except Exception as e:
        logging.error(f"Failed to upload tweets.csv: {e}")