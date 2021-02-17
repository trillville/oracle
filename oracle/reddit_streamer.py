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
        self.comments_to_update = []
        self.comments_jobs = deque(self.r.keys().reverse() or [])
        self.timers = {
            "finding_keywords": 0.0,
            "redis_set": 0.0,
            "insert_and_update_pg": 0.0,
            "update_only_pg": 0.0,
            "check_exists": 0.0,
            "pull_posts": 0.0,
            "pull_comments": 0.0,
        }

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
                "parent_id": comment.parent_id[3:],
            }
        )
        if len(keywords) > 0:
            s = time.time()
            self.r.set(name=comment.id, value="", ex=2 * 24 * 60 * 60)
            self.comments_jobs.appendleft(comment.id)
            self.timers["redis_set"] += time.time() - s
        if len(self.comments) >= 50:
            s = time.time()
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
                    UPDATE comments SET comments = comments + 1 where id = %(parent_id)s;
                """,
                    ({**tmp_comment} for tmp_comment in self.comments),
                )
            self.comments = []
            self.timers["insert_and_update_pg"] += time.time() - s

    def update_comment(self):
        try:
            comment_id = self.comments_jobs.pop()
            s = time.time()
            self.comments_to_update.append(
                {
                    "id": comment_id,
                    "ups": self.reddit.comment(id=comment_id).ups,
                    "time": datetime.now(),
                }
            )
            self.timers["pull_comments"] += time.time() - s
            s = time.time()
            if self.r.exists(comment_id):
                self.timers["check_exists"] += time.time() - s
                self.comments_jobs.appendleft(comment_id)
        except:
            pass

        if len(self.comments_to_update) >= 50:
            s = time.time()
            with self.connection.cursor() as cursor:
                psycopg2.extras.execute_batch(
                    cursor,
                    f"""
                    UPDATE comments
                    SET
                        upvotes = %(ups)s,
                        last_updated = %(time)s
                    WHERE
                        id = %(id)s;
                """,
                    ({**update_key} for update_key in self.comments_to_update),
                )
            self.comments_to_update = []
            self.timers["update_only_pg"] += time.time() - s


def main():
    streamer = RedditStreamer()
    c, p, u = 0, 0, 0

    while True:
        for post in streamer.posts_stream:
            s = time.time()
            if post is None:
                break
            p += 1
            streamer.timers["pull_posts"] += time.time() - s
            streamer.insert_post(post)

        for comment in streamer.comments_stream:
            s = time.time()
            if comment is None:
                break
            c += 1
            streamer.timers["pull_comments"] += time.time() - s
            streamer.insert_comment(comment)

        u += 2
        streamer.update_comment()
        streamer.update_comment()

        total = c + p + u
        if total % 100 == 0:
            print(f"comments added: {c}, posts added: {p}, comments updated: {u}")
            print(streamer.timers)


if __name__ == "__main__":
    main()
