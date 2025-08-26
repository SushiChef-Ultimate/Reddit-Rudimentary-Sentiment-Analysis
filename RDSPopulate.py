import boto3
import json
import praw
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, DateTime
from sqlalchemy.dialects.postgresql import insert

def runFile():
    def fetch_secrets(secret_name, region="us-east-2"):
        client = boto3.client("secretsmanager", region_name=region)
        return json.loads(client.get_secret_value(SecretId=secret_name)["SecretString"])

    # --- Fetch secrets from AWS Secrets Manager ---
    reddit_secrets = fetch_secrets("reddit-scraper-secrets")
    rds_secrets = fetch_secrets("rds!db-027311fd-e11d-4e69-919d-7eef98ca22a6")

    # --- Use secrets directly ---
    CLIENT_ID = reddit_secrets["CLIENT_ID"]
    CLIENT_SECRET = reddit_secrets["CLIENT_SECRET"]
    USER_AGENT = reddit_secrets["USER_AGENT"]

    DB_USER = rds_secrets["username"]
    DB_PASS = rds_secrets["password"]
    DB_HOST = rds_secrets["host"]
    DB_PORT = rds_secrets["port"]
    DB_NAME = rds_secrets["dbname"]

    RDS_DB_CONNECTION = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


    # --- Connect to Reddit ---
    reddit = praw.Reddit(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        user_agent=USER_AGENT
    )

    # --- Fetch latest 50 posts ---
    subreddit = reddit.subreddit("economics")
    posts = []
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

    df = pd.DataFrame(posts)
    df["author"] = df["author"].apply(lambda a: None if a == "None" else a)
    df["created_at"] = pd.to_datetime(df["created_at"], unit='s', utc=True)

    # --- Connect to AWS RDS PostgreSQL ---
    engine = create_engine(RDS_DB_CONNECTION, pool_pre_ping=True)

    # --- Create table if it doesn't exist ---
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

    # --- Define table metadata for upserts ---
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

    # --- Preview latest 5 posts ---
    with engine.begin() as conn:
        preview = pd.read_sql(
            "SELECT id, title, score, num_comments, created_at FROM reddit_posts ORDER BY created_at DESC LIMIT 5",
            conn
        )
    print(preview)

if __name__ == "__main__":
    runFile()