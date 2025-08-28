import logging
import pandas as pd
import praw
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, DateTime
from sqlalchemy.dialects.postgresql import insert
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime, timezone

# Configure logging so we can see what's happening in AZ functions
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KEYVAULT_URL = "https://Reddit-Keys.vault.azure.net/" # Key vault to grab our secrets so we can connect to our reddit scraper and database

def get_secret(secret_name: str) -> str: # return hints like these help with finding out about errors
    credential = DefaultAzureCredential() # Imported function that helps with the transfer of secrets while keeping things secure
    client = SecretClient(vault_url=KEYVAULT_URL, credential=credential)
    secret = client.get_secret(secret_name)
    return secret.value

def main(mytimer: func.TimerRequest) -> None: # Time-controlled azure function controlled by function.json
    try:
        logger.info("Azure Function started at %s", datetime.now(timezone.utc))

        CLIENT_ID = get_secret("CLIENT-ID")
        CLIENT_SECRET = get_secret("CLIENT-SECRET")
        USER_AGENT = get_secret("USER-AGENT")

        DB_USER = get_secret("DB-USER")
        DB_PASS = get_secret("DB-PASS")
        DB_HOST = get_secret("DB-HOST")
        DB_PORT = get_secret("DB-PORT")
        DB_NAME = get_secret("DB-NAME")
        reddit = praw.Reddit(client_id=CLIENT_ID,
                             client_secret=CLIENT_SECRET,
                             user_agent=USER_AGENT)
        subreddit = reddit.subreddit("economics")  # We'll be scraping the economics subreddit
        posts = []
        for post in subreddit.hot(limit=100):
            posts.append({
                "id": post.id,
                "title": post.title,
                "author": str(post.author),
                "score": post.score,
                "num_comments": post.num_comments,
                "created_at": post.created_utc,
                "url": post.url
            })

        # A few important steps here!
        # We first assign a panda dataframe to our posts list of dictionaries, which helps with sql insertion engines later
        # If a reddit user who made a certain post deletes their account, praw will return "None", so we ensure its a string for consistency
        # We convert the "created at" value from praw to utc, so we can sort by that value
        df = pd.DataFrame(posts)
        df["author"] = df["author"].apply(lambda a: None if a == "None" else a)
        df["created_at"] = pd.to_datetime(df["created_at"], unit='s', utc=True)
        logger.info("Fetched %d posts from Reddit", len(df))

        conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}" # Assembling the connection string to DB using our secrets!
        # We create this engine to carry out our SQL insertions. Pool_pre_ping=True has helped me prevent the occasional "OperationalError"
        engine = create_engine(conn_str, pool_pre_ping=True)

        # This sql creates a table in our DB if one doesnt exist, doesn't really run most of the time
        create_sql = """
        CREATE TABLE IF NOT EXISTS reddit_posts (
            id TEXT PRIMARY KEY,
            title TEXT,
            author TEXT,
            score INTEGER,
            num_comments INTEGER,
            created_at TIMESTAMPTZ,
            url TEXT
        );
        """

        with engine.begin() as conn:
            conn.execute(text(create_sql))
        logger.info("Table ensured")

        # Metadata for how rows should be inserted and their columns. We highlight "id" as a primary key (important for later)
        metadata = MetaData()
        reddit_posts = Table(
            "reddit_posts", metadata,
            Column("id", String, primary_key=True),
            Column("title", String),
            Column("author", String),
            Column("score", Integer),
            Column("num_comments", Integer),
            Column("created_at", DateTime(timezone=True)),
            Column("url", String)
        )
        # The orient function allows us to alter our dataframe entities explicitly into records
        # This allows for easy mapping of key-values when inserting this data in our table
        # Critically, is we detect an id of a post we already inserted, instead of inserting we update the post with the new engagement scores we have
        records = df.to_dict(orient="records")
        stmt = insert(reddit_posts).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=[reddit_posts.c.id],
            set_={
                "title": stmt.excluded.title,
                "author": stmt.excluded.author,
                "score": stmt.excluded.score,
                "num_comments": stmt.excluded.num_comments,
                "created_at": stmt.excluded.created_at,
                "url": stmt.excluded.url,
            }
        )
        
        with engine.begin() as conn:
            conn.execute(stmt)
        logger.info("Inserted/updated %d records", len(records))

        logger.info("Azure Function finished successfully")

    except Exception as e:
        logger.error("Error in Azure Function: %s", e)