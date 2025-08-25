import praw
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime
from sqlalchemy.dialects.postgresql import insert

# We first load .env info into variables
def runFile():
    load_dotenv()

    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")
    user_agent = os.getenv("USER_AGENT")

    reddit = praw.Reddit( # connect to reddit API using .env variables
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )

    subreddit = reddit.subreddit("economics") # set our subreddit to "economics"
    posts = [] # List of posts
    for post in subreddit.hot(limit=50):
        posts.append({
            "id": post.id,
            "title": post.title,
            "author": str(post.author),
            "score": post.score,
            "num_comments": post.num_comments,
            "created_at": post.created_utc,
            "url": post.url
        })

    PandaDataframe = pd.DataFrame(posts) # Using pandas, we convert posts list to tabular data format

    # Next we need to clean up our data a little before export
    PandaDataframe["author"] = PandaDataframe["author"].apply(lambda a: None if a == "None" else a) # Changes posts with no author to NULL
    PandaDataframe["created_at"] = pd.to_datetime(PandaDataframe["created_at"], unit='s', utc=True) # Converts reddit timestamp to more usable datetime

    NEON_DB_CONNECTION = os.getenv("NEON_DB_CONNECTION")
    engine = create_engine(NEON_DB_CONNECTION, pool_pre_ping=True)

    # Creation of table, if it doesnt exist
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

    # Applies created table if table doesn't exist, makes no changes if table does already exist
    with engine.begin() as x:
        x.execute(text(create_sql))

    # Define metadata for PostgreSQL table, since table schema isnt remembered between each SQL execution
    metadata = MetaData()
    reddit_posts = Table(
        "reddit_posts", metadata,
        Column("id", String, primary_key=True), # We set this column as primary key, since this is the column that will be used to check for conflicts
        Column("title", String),
        Column("author", String),
        Column("score", Integer),
        Column("num_comments", Integer),
        Column("created_at", DateTime(timezone=True)),
        Column("url", String),
    )

    # Organizes all dataframe rows into a dictionary format, allowing for easier mapping to the table
    records = PandaDataframe.to_dict(orient="records")

    # Try to insert stmt as a new row. If conflict occurs, we pull data from the excluded row to update the existing one
    try:
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
            },
        )
        # Executes stmt and populates the row
        with engine.begin() as x:
            x.execute(stmt)

    except: # Deletion for when database if full. We can do a simple deletion to continue the process. CycleDatabase.py is prefferable for real cycling.
        delete = """
        DELETE FROM reddit_posts 
        WHERE id IN (
            SELECT ID
            FROM reddit_posts
            ORDER BY created_at ASC
            LIMIT 50
        );
        """

        # Executes deletion
        with engine.begin() as deleter:
            deleter.execute(text(delete))
        print("DB is full, deleted old rows and made space!")


    # Final checkover to verify our data
    with engine.begin() as conn:
        preview = pd.read_sql(
            "SELECT id, title, score, num_comments, created_at FROM reddit_posts ORDER BY created_at DESC LIMIT 5",
            conn
        )
    print(preview)

if __name__ == "__main__":
    runFile()