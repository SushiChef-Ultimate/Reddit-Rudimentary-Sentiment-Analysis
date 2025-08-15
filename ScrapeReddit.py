import praw
import os
from dotenv import load_dotenv

# We first load .env info into variables
load_dotenv()

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
user_agent = os.getenv("USER_AGENT")

reddit = praw.Reddit( # connect to reddit API using .env variables
    client_id=client_id,
    client_secret=client_secret,
    user_agent=user_agent
)