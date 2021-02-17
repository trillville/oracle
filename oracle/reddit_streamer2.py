import os
import praw
from flashtext import KeywordProcessor
from textblob import TextBlob
from tickers import NYSE, NASDAQ, AMEX
from cachetools import TTLCache
from collections import deque
import psycopg2
import psycopg2.extras
from datetime import datetime


class RedditStreamer:
    def __init__(self):
        self.connection = psycopg2.connect(os.environ["DATABASE_URL"], sslmode="require")
        self.connection.autocommit = True
        self.reddit = praw.Reddit(
            client_id=os.environ["REDDIT_CLIENT_ID"],
            client_secret=os.environ["REDDIT_CLIENT_SECRET"],
            user_agent=os.environ["REDDIT_USER_AGENT"],
        )
        self.posts_stream = self.reddit.subreddit("wallstreetbets").stream.submissions(
            pause_after=-1, skip_existing=True
        )
        self.comments_stream = self.reddit.subreddit("wallstreetbets").stream.comments(
            pause_after=-1, skip_existing=True
        )
        self.keyword_processor = KeywordProcessor()
        self.keyword_processor.add_keywords_from_list(NYSE + NASDAQ + AMEX)
        self.comments = []
        self.active_comments = TTLCache(maxsize=50000, ttl=60 * 60 * 48)
        self.comments_jobs = deque()
        self.comments_to_update = []

    def insert_post(self, submission):
        title_keywords = [
            x.replace("$", "") for x in self.keyword_processor.extract_keywords(submission.title)
        ]
        text_keywords = [
            x.replace("$", "") for x in self.keyword_processor.extract_keywords(submission.selftext)
        ]
        sub = {
            "posted": datetime.utcfromtimestamp(submission.created_utc),
            "last_updated": datetime.utcfromtimestamp(submission.created_utc),
            "id": submission.id,
            "title": submission.title,
            "title_mentions": list(set(title_keywords)),
            "text_mentions": list(set(text_keywords)),
            "sentiment": TextBlob(submission.title).sentiment.polarity,
            "upvotes": submission.ups,
            "comments": submission.num_comments,
        }
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO posts VALUES (
                %(posted)s,
                %(last_updated)s,
                %(id)s,
                %(title)s,
                %(title_mentions)s,
                %(text_mentions)s,
                %(sentiment)s,
                %(upvotes)s,
                %(comments)s
                );
            """,
                {**sub},
            )

    def insert_comment(self, comment):
        keywords = [
            x.replace("$", "") for x in self.keyword_processor.extract_keywords(comment.body)
        ]
        self.comments.append(
            {
                "posted": datetime.utcfromtimestamp(comment.created_utc),
                "last_updated": datetime.utcfromtimestamp(comment.created_utc),
                "id": comment.id,
                "text": comment.body[:50],
                "text_mentions": list(set(keywords)),
                "sentiment": TextBlob(comment.body).sentiment.polarity,
                "upvotes": comment.ups,
                "comments": 0,
            }
        )
        if len(keywords) > 0:
            self.active_comments[comment.id] = None
            self.comments_jobs.appendleft(comment.id)
        with self.connection.cursor() as cursor:
            cursor.execute(
                "UPDATE comments SET comments = comments + 1 where id = %s;",
                [comment.parent_id[3:]],
            )
        if len(self.comments) >= 10:
            with self.connection.cursor() as cursor:
                psycopg2.extras.execute_batch(
                    cursor,
                    """
                    INSERT INTO comments VALUES (
                    %(posted)s,
                    %(last_updated)s,
                    %(id)s,
                    %(text)s,
                    %(text_mentions)s,
                    %(sentiment)s,
                    %(upvotes)s,
                    %(comments)s
                    );
                """,
                    ({**tmp_comment} for tmp_comment in self.comments),
                )
            self.comments = []
            print("comments added")

    def update_comment(self):
        print(len(self.comments_to_update), len(self.comments_jobs), len(self.active_comments))
        comment_id = self.comments_jobs.pop()
        try:
            self.comments_to_update[id] = self.reddit.comment(id=comment_id).ups
            if comment_id in self.active_comments:
                self.comments_jobs.appendleft(comment_id)
            print(len(self.comments_to_update))
        except:
            pass

        if len(self.comments_to_update) >= 10:
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
                    [
                        (self.comments_to_update[key], datetime.now(), key)
                        for key in self.comments_to_update.keys()
                    ],
                )
            print("comments updated")


def main():
    streamer = RedditStreamer()

    while True:
        for post in streamer.posts_stream:
            if post is None:
                break
            print("post updated")
            streamer.insert_post(post)

        for comment in streamer.comments_stream:
            if comment is None:
                break
            streamer.insert_comment(comment)

        if len(streamer.comments_jobs) > 0:
            streamer.update_comment()


if __name__ == "__main__":
    main()
