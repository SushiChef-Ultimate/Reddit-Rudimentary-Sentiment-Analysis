import boto3
import json
import praw
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, DateTime
from sqlalchemy.dialects.postgresql import insert

def runFile():
    # --- Fetch secrets from AWS Secrets Manager ---
    secret_name = "reddit-rds-secret"  # Replace with your secret name
    client = boto3.client("secretsmanager")
    secret_value = client.get_secret_value(SecretId=secret_name)
    secrets = json.loads(secret_value["SecretString"])

    CLIENT_ID = secrets["CLIENT_ID"]
    CLIENT_SECRET = secrets["CLIENT_SECRET"]
    USER_AGENT = secrets["USER_AGENT"]
    RDS_DB_CONNECTION = secrets["RDS_DB_CONNECTION"]

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
            }
        )
        with engine.begin() as conn:
            conn.execute(stmt)

    except Exception as e:
        print(f"Conflict insert failed, cycling old rows. Error: {e}")
        delete_sql = """
        DELETE FROM reddit_posts 
        WHERE id IN (
            SELECT id
            FROM reddit_posts
            ORDER BY created_at ASC
            LIMIT 50
        );
        """
        with engine.begin() as conn:
            conn.execute(text(delete_sql))

    # --- Cycle old rows equal to new inserts ---
    new_ids = [r["id"] for r in records]
    with engine.begin() as conn:
        existing_ids = conn.execute(
            text("SELECT id FROM reddit_posts WHERE id = ANY(:ids)"),
            {"ids": new_ids}
        )
        existing_ids_count = [r[0] for r in existing_ids]

    new_count = len(new_ids) - len(existing_ids_count)

    if new_count > 0:
        cycle_sql = """
        DELETE FROM reddit_posts 
        WHERE id IN (
            SELECT id
            FROM reddit_posts
            ORDER BY created_at ASC
            LIMIT :new_inserted
        );
        """
        with engine.begin() as conn:
            conn.execute(text(cycle_sql), {"new_inserted": new_count})
        print(f"Successfully cycled {new_count} old rows!")

    # --- Preview latest 5 posts ---
    with engine.begin() as conn:
        preview = pd.read_sql(
            "SELECT id, title, score, num_comments, created_at FROM reddit_posts ORDER BY created_at DESC LIMIT 5",
            conn
        )
    print(preview)

if __name__ == "__main__":
    runFile()