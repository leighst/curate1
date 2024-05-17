# 1. start_date, end_date, batch_size, overwrite/skip
# 2. produce a dataset one at a time - one fn per dataset
# 3. datasets represented as df
# 4.

import requests
import json
from selenium import webdriver
import re
from bs4 import BeautifulSoup

class Pipeline:
  def __init__(self, db):
    self.db = db
  
  def load_hackernews_posts_range(self, start_index, end_index, batch_size=10, overwrite=False):
    self.load_hackernews_posts_batch(start_index, batch_size, overwrite)

  def load_hackernews_posts_ids(self, post_ids, overwrite=False):
    print(f"Loading posts {post_ids}")
    posts = []

    for id in post_ids:
      post = self.fetch_hn_post(str(id))
      if post is not None:
        posts.append(post)  
    
    self.db.insert_posts(posts)

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

  def load_hackernews_contents_ids(self, post_ids, overwrite=False):
    posts = self.db.get_posts_by_ids(post_ids)
    attributes = self.fetch_hn_content_urls(posts)
    self.db.insert_doc_attributes(attributes, "hn_content")
    pass

  def fetch_hn_content_urls(self, posts):
    driver = webdriver.Chrome()
    
    post_contents = []
    for post in posts:
      print(f"Fetching content for {post}")

      post_content = self.fetch_hn_content(driver, post['url'])
      post_contents.append({
        "post_id": post['id'],
        "value": post_content
      })

    driver.quit()
    return post_contents

  def fetch_hn_content(self, driver, post_url):
    driver.get(post_url)
    driver.implicitly_wait(10)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    text = soup.get_text()
    return re.sub('(\s|\\\\n)+', ' ', text)
