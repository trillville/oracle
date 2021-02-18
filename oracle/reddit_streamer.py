import os
import praw
from flashtext import KeywordProcessor
from textblob import TextBlob
from tickers import NYSE, NASDAQ, AMEX
from collections import deque
import psycopg2
import psycopg2.extras
from datetime import datetime
import redis
import time


class RedditStreamer:
    def __init__(self):
        self.r = redis.StrictRedis.from_url(os.environ.get("REDIS_URL"))
        self.r.set_response_callback('GET', int)
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
        self.jobs = deque(self.r.keys().reverse() or [])
        self.update_batch = []

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
            "id": submission.name,
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
                INSERT INTO updates VALUES (
                %(posted)s,
                %(last_updated)s,
                %(id)s,
                %(upvotes)s,
                %(comments)s
                );
            """,
                {**sub},
            )
            if len(title_keywords) > 0 or len(text_keywords) > 0:
                self.r.set(name=submission.name, value=0, ex=2 * 24 * 60 * 60)
                self.jobs.appendleft(submission.name)

    def insert_comment(self, comment):
        keywords = [
            x.replace("$", "") for x in self.keyword_processor.extract_keywords(comment.body)
        ]
        self.comments.append(
            {
                "posted": datetime.utcfromtimestamp(comment.created_utc),
                "last_updated": datetime.utcfromtimestamp(comment.created_utc),
                "id": comment.name,
                "text": comment.body[:50],
                "text_mentions": list(set(keywords)),
                "sentiment": TextBlob(comment.body).sentiment.polarity,
                "upvotes": comment.ups,
                "comments": 0,
            }
        )
        parent_id = comment.parent_id
        if parent_id is not None and self.r.exists(parent_id):
            self.r.incr(parent_id)

        if len(keywords) > 0:
            self.r.set(name=comment.name, value=0, ex=2 * 24 * 60 * 60)
            self.jobs.appendleft(comment.name)


        if len(self.comments) >= 100:
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
                    INSERT INTO updates VALUES (
                    %(posted)s,
                    %(last_updated)s,
                    %(id)s,
                    %(upvotes)s,
                    %(comments)s
                    );
                """,
                    ({**tmp_comment} for tmp_comment in self.comments),
                )
            self.comments = []

    def update_comment(self):
        print(f"joblist size: {len(self.jobs)}")
        for i in range(min(20, len(self.jobs))):
            self.update_batch.append(self.jobs.pop())
        if len(self.update_batch) >= 100:
            updates = []
            for item in self.reddit.info(fullnames=self.update_batch):
                if item.name.startswith("t3"):
                    self.t3 += 1
                    num_comments = item.num_comments
                else:
                    self.t1 += 1
                    num_comments = self.r.get(item.name) or 0
                updates.append(
                    {
                        "posted": datetime.utcfromtimestamp(item.created_utc),
                        "last_updated": datetime.now(),
                        "id": item.name,
                        "upvotes": item.ups,
                        "comments": num_comments
                    }
                )
                if self.r.exists(item.name):
                    self.comments_jobs.appendleft(item.name)
            with self.connection.cursor() as cursor:
                psycopg2.extras.execute_batch(
                    cursor,
                    """
                    INSERT INTO updates VALUES (
                    %(posted)s,
                    %(last_updated)s,
                    %(id)s,
                    %(upvotes)s,
                    %(comments)s
                    );
                """,
                    ({**tmp_comment} for tmp_comment in updates),
                )
            self.update_batch = []

def main():
    streamer = RedditStreamer()
    c, p = 0, 0
    streamer.t3 = 0
    streamer.t1 = 0
    overall_start = time.time()

    while True:
        for post in streamer.posts_stream:
            if post is None:
                break
            p += 1
            streamer.insert_post(post)

        for comment in streamer.comments_stream:
            if comment is None:
                break
            c += 1
            streamer.insert_comment(comment)

        streamer.update_comment()

        total = c + p + streamer.t1 + streamer.t3
        if streamer.t1 % 100 == 0:
            print(f"comments added: {c}, posts added: {p}, comments updated: {streamer.t1}, posts updated: {streamer.t3}")
            print(f"APS: {total / (time.time() - overall_start)}")


if __name__ == "__main__":
    main()
