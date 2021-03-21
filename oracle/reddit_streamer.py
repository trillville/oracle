import os
import praw
from prawcore.exceptions import ServerError
from flashtext import KeywordProcessor
from textblob import TextBlob
from tickers import NYSE, NASDAQ, AMEX
from collections import deque
import psycopg2
import psycopg2.extras
from datetime import datetime
import redis
import time
from time import time, sleep

import watchtower, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("wsb-streamer")
logger.addHandler(watchtower.CloudWatchLogHandler())


class RedditStreamer:
    def __init__(self):
        self.r = redis.StrictRedis(
            host=os.environ.get("REDIS_HOST"), charset="utf-8", decode_responses=True
        )
        self.jobs = deque(self.r.keys() or [])
        self.connection = psycopg2.connect(
            host=os.environ["RDS_HOST"], user=os.environ["RDS_USER"], password=os.environ["RDS_PW"]
        )
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
            self.r.set(name=comment.name, value=0, ex=36 * 60 * 60)
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
                """,
                    ({**tmp_comment} for tmp_comment in self.comments),
                )
            self.comments = []

    def update_comments(self):
        updates = []
        for item in self.reddit.info(fullnames=self.update_batch):
            if item.name.startswith("t3"):
                self.t3 += 1
                num_comments = item.num_comments
            elif item.name.startswith("t1"):
                self.t1 += 1
                num_comments = int(self.r.get(item.name) or 0)
            else:
                logger.info(f"DEBUG: {item.name}")
                num_comments = 0
            updates.append(
                {
                    "posted": datetime.utcfromtimestamp(item.created_utc),
                    "last_updated": datetime.now(),
                    "id": item.name,
                    "upvotes": item.ups,
                    "comments": num_comments,
                }
            )
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
    overall_start = time()
    inc = 0

    while True:
        try:
            inc += 1
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
        except Exception as e:
            logger.info(f"Reddit API Error: {e}")
            streamer.posts_stream = streamer.reddit.subreddit("wallstreetbets").stream.submissions(
                pause_after=-1, skip_existing=True
            )
            streamer.comments_stream = streamer.reddit.subreddit("wallstreetbets").stream.comments(
                pause_after=-1, skip_existing=True
            )
            sleep(10)

        for i in range(min(50, len(streamer.jobs))):
            id = streamer.jobs.pop()
            streamer.update_batch.append(id)
            if streamer.r.exists(id):
                streamer.jobs.appendleft(id)

        if len(streamer.update_batch) >= 100:
            try:
                streamer.update_comments()
            except ServerError as e:
                logger.info(f"Reddit Server Error: {e}")
                sleep(10)

        if inc % 100 == 0:
            logger.info(
                f"comments added: {c}, posts added: {p}, comments updated: {streamer.t1}, posts updated: {streamer.t3}"
            )
            logger.info(f"APS: {(c + p + streamer.t3 + streamer.t1) / (time() - overall_start)}")


if __name__ == "__main__":
    main()
