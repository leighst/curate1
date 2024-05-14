# 1. start_date, end_date, batch_size, overwrite/skip
# 2. produce a dataset one at a time - one fn per dataset
# 3. datasets represented as df
# 4.

import requests
import json

class Pipeline:
  def __init__(self, db):
    self.db = db
  
  def load_hackernews_posts(self, start_index, end_index, batch_size=10, overwrite=False):
    self.load_hackernews_posts_batch(start_index, batch_size, overwrite)

  def load_hackernews_posts_batch(self, start_index, batch_size, overwrite=False):
    print(f"Loading {batch_size} posts beginning with {start_index}")
    posts = []

    ids = list(range(start_index, start_index+batch_size))
    for id in ids:
      post = self.fetch_hn_post(str(id))
      if post is not None:
        posts.append(post)  
    
    self.db.insert_posts(posts)

  def get_max_item_id(self):
    res = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json")
    return res.json()

  def fetch_hn_post(self, id):
    uri = f"https://hacker-news.firebaseio.com/v0/item/{id}.json"
    print(f"Loading {uri}")

    res = requests.get(uri)
    if res.status_code != 200:
      print(f"Request failed with {res.status_code}: {res.reason}")
      return None

    data = res.json()
    print(f"Got hacker news post {json.dumps(data)}")

    if data.get('type') != "story" or not data.get('url'):
      print("Not a story, skipping...")
      return None

    return data
