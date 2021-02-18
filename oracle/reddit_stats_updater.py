import os
import praw
import psycopg2
import psycopg2.extras
import time
from datetime import datetime, timedelta


class RedditUpdater:
    def __init__(self):
        self.connection = psycopg2.connect(os.environ["DATABASE_URL"], sslmode="require")
        self.connection.autocommit = True
        self.reddit = praw.Reddit(
            client_id=os.environ["REDDIT_CLIENT_ID"],
            client_secret=os.environ["REDDIT_CLIENT_SECRET"],
            user_agent=os.environ["REDDIT_USER_AGENT"],
        )

    def pull_ids(self, table_name):
        where_clause = ""
        if table_name == "comments":
            where_clause = "WHERE cardinality(text_mentions) > 0"
            records = 1000
        elif table_name == "posts":
            where_clause = "WHERE cardinality(text_mentions) > 0 OR cardinality(title_mentions) > 0"
            records = 100
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT id from {table_name} {where_clause} ORDER BY last_updated ASC LIMIT {records}")
            ids = [x[0] for x in cursor.fetchall()]
            update_dict = {id: {"upvotes": 0, "comments": 0} for id in ids}
            return ids, update_dict

    def delete_old(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
                DELETE FROM posts
                WHERE posted < %s;
                DELETE FROM comments
                WHERE posted < %s;
                DELETE FROM updates
                WHERE posted < %s;
                """,
                [datetime.now() - timedelta(hours=120), datetime.now() - timedelta(hours=120), datetime.now() - timedelta(hours=120],
            )

    def update_posts(self, update_dict):
        with self.connection.cursor() as cursor:
            psycopg2.extras.execute_batch(
                cursor,
                f"""
                UPDATE posts
                SET
                    upvotes = %s,
                    comments = %s,
                    last_updated = %s
                WHERE
                    id = %s;
            """,
                [
                    (update_dict[key]["upvotes"], update_dict[key]["comments"], datetime.now(), key)
                    for key in update_dict.keys()
                ],
            )

    def update_comments(self, update_dict):
        with self.connection.cursor() as cursor:
            psycopg2.extras.execute_batch(
                cursor,
                f"""
                UPDATE comments
                SET
                    upvotes = %s,
                    last_updated = %s
                WHERE
                    id = %s;
            """,
                [(update_dict[key]["upvotes"], datetime.now(), key) for key in update_dict.keys()],
            )

    def update_tickers(self):
        dt = {"min_datetime": datetime.now() - timedelta(hours=47)}
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""
            WITH comment_data AS (
              SELECT date, ticker, AVG(sentiment) as comment_sentiment, SUM(upvotes) AS comment_upvotes, SUM(comments) AS comment_replies, COUNT(DISTINCT id) as comment_mentions
              FROM (SELECT id, date_trunc('hour', posted) as date, UNNEST(text_mentions) as ticker, sentiment, upvotes, comments from comments WHERE posted > %(min_datetime)s) a
              GROUP BY date, ticker
            ),
            post_title_data AS (
              SELECT date, ticker, AVG(sentiment) as post_title_sentiment, SUM(upvotes) AS post_title_upvotes, SUM(comments) AS post_title_replies, COUNT(DISTINCT id) as post_title_mentions
              FROM (SELECT id, date_trunc('hour', posted) as date, UNNEST(title_mentions) as ticker, sentiment, upvotes, comments from posts WHERE posted > %(min_datetime)s) a
              GROUP BY date, ticker
            ),
            post_text_data AS (
              SELECT date, ticker, AVG(sentiment) as post_text_sentiment, SUM(upvotes) AS post_text_upvotes, SUM(comments) AS post_text_replies, COUNT(DISTINCT id) as post_text_mentions
              FROM (SELECT id, date_trunc('hour', posted) as date, UNNEST(text_mentions) as ticker, sentiment, upvotes, comments from posts WHERE posted > %(min_datetime)s) a
              GROUP BY date, ticker
            ),
            post_data_combined AS (
            SELECT
              COALESCE(a.date, b.date) AS date,
              COALESCE(a.ticker, b.ticker) AS ticker,
              post_title_sentiment,
              post_text_sentiment,
              COALESCE(post_title_upvotes, 0) AS post_title_upvotes,
              COALESCE(post_text_upvotes, 0) AS post_text_upvotes,
              COALESCE(post_title_replies, 0) AS post_title_replies,
              COALESCE(post_text_replies, 0) AS post_text_replies,
              COALESCE(post_title_mentions, 0) AS post_title_mentions,
              COALESCE(post_text_mentions, 0) AS post_text_mentions
            FROM post_title_data a
            FULL OUTER JOIN post_text_data b
              ON a.date = b.date AND a.ticker = b.ticker
            ),
            ticker_data AS (
            SELECT
              COALESCE(a.date, b.date) AS date,
              COALESCE(a.ticker, b.ticker) AS ticker,
              comment_sentiment,
              post_title_sentiment,
              post_text_sentiment,
              COALESCE(comment_upvotes, 0) AS comment_upvotes,
              COALESCE(post_title_upvotes, 0) AS post_title_upvotes,
              COALESCE(post_text_upvotes, 0) AS post_text_upvotes,
              COALESCE(comment_replies, 0) AS comment_replies,
              COALESCE(post_title_replies, 0) AS post_title_replies,
              COALESCE(post_text_replies, 0) AS post_text_replies,
              COALESCE(comment_mentions, 0) AS comment_mentions,
              COALESCE(post_title_mentions, 0) AS post_title_mentions,
              COALESCE(post_text_mentions, 0) AS post_text_mentions
            FROM comment_data a
            FULL OUTER JOIN post_data_combined b
              ON a.date = b.date AND a.ticker = b.ticker
            )
            INSERT INTO tickers (date, ticker, comment_sentiment, post_title_sentiment, post_text_sentiment,
                                 comment_upvotes, post_title_upvotes, post_text_upvotes, comment_replies,
                                 post_title_replies, post_text_replies, comment_mentions,
                                 post_title_mentions, post_text_mentions)
            SELECT * FROM ticker_data
            ON CONFLICT (date, ticker)
            DO UPDATE SET
              comment_sentiment = EXCLUDED.comment_sentiment,
              post_title_sentiment = EXCLUDED.post_title_sentiment,
              post_text_sentiment = EXCLUDED.post_text_sentiment,
              comment_upvotes = EXCLUDED.comment_upvotes,
              post_title_upvotes = EXCLUDED.post_title_upvotes,
              post_text_upvotes = EXCLUDED.post_text_upvotes,
              comment_replies = EXCLUDED.comment_replies,
              post_title_replies = EXCLUDED.post_title_replies,
              post_text_replies = EXCLUDED.post_text_replies,
              comment_mentions = EXCLUDED.comment_mentions,
              post_title_mentions = EXCLUDED.post_title_mentions,
              post_text_mentions = EXCLUDED.post_text_mentions;
            """,
                {**dt},
            )


def main():
    updater = RedditUpdater()
    updater.delete_old()
    #
    # start = time.time()
    # updater.update_tickers()
    # print(f"Tickers Updated! Time Spent: {time.time() - start}")


if __name__ == "__main__":
    main()
