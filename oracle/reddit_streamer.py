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
        self.comments_jobs = deque(self.r.keys().reverse() or [])
        self.timers = {
            "finding_keywords": 0.0,
            "redis_set": 0.0,
            "insert_pg": 0.0,
            "check_exists": 0.0,
            "pull_posts": 0.0,
            "pull_comments": 0.0,
        }
        self.comment_update_batch = []

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
        s = time.time()
        keywords = [
            x.replace("$", "") for x in self.keyword_processor.extract_keywords(comment.body)
        ]
        self.timers["finding_keywords"] += time.time() - s
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
        parent_id = comment.parent_id[3:]
        if parent_id is not None and self.r.exists(parent_id):
            self.r.incr(parent_id)

        if len(keywords) > 0:
            s = time.time()
            self.r.set(name=comment.id, value=0, ex=2 * 24 * 60 * 60)
            self.comments_jobs.appendleft(comment.id)
            self.timers["redis_set"] += time.time() - s


    def update_comment(self):
        for i in range(10):
            try:
                self.comment_update_batch.append("t1_" + self.comments_jobs.pop())
            except:
                pass
        if len(self.comment_update_batch) >= 100:
            s = time.time()
            for comment in self.reddit.info(fullnames=self.comment_update_batch):
                keywords = [
                    x.replace("$", "") for x in self.keyword_processor.extract_keywords(comment.body)
                ]
                self.comments.append(
                    {
                        "posted": datetime.utcfromtimestamp(comment.created_utc),
                        "last_updated": datetime.now(),
                        "id": comment.id,
                        "text": comment.body[:50],
                        "text_mentions": list(set(keywords)),
                        "sentiment": TextBlob(comment.body).sentiment.polarity,
                        "upvotes": comment.ups,
                        "comments": self.r.get(comment.id) or 0
                    }
                )
                s = time.time()
                if self.r.exists(comment.id):
                    self.comments_jobs.appendleft(comment.id)
                self.timers["check_exists"] += time.time() - s
            self.timers["pull_comments"] += time.time() - s
            self.comment_update_batch = []

def main():
    streamer = RedditStreamer()
    c, p, u = 0, 0, 0
    overall_start = time.time()

    while True:
        s = time.time()
        for post in streamer.posts_stream:
            if post is None:
                break
            p += 1
            streamer.timers["pull_posts"] += time.time() - s
            streamer.insert_post(post)
            s = time.time()

        s = time.time()
        for comment in streamer.comments_stream:
            if comment is None:
                break
            c += 1
            streamer.timers["pull_comments"] += time.time() - s
            streamer.insert_comment(comment)
            s = time.time()

        u += 10
        streamer.update_comment()

        if len(streamer.comments) >= 100:
            s = time.time()
            with streamer.connection.cursor() as cursor:
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
                    ({**tmp_comment} for tmp_comment in streamer.comments),
                )
            streamer.comments = []
            streamer.timers["insert_pg"] += time.time() - s

        total = c + p + u
        if c+p % 100 == 0:
            print(f"comments added: {c}, posts added: {p}, comments updated: {u}")
            print(streamer.timers)
            print(f"APS: {total / (time.time() - overall_start)}")


if __name__ == "__main__":
    main()
