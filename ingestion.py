# 1. start_date, end_date, batch_size, overwrite/skip
# 2. produce a dataset one at a time - one fn per dataset
# 3. datasets represented as df
# 4.

import requests
import json
from selenium import webdriver
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from newspaper import Article

class Pipeline:
  def __init__(self, db):
    self.db = db
  
  def load_hackernews_posts_range(self, start_index, end_index, batch_size=10, overwrite=False):
    if not overwrite:
      latest_index = self.db.get_max_item_id()
      start_index = latest_index + 1 if latest_index is not None else start_index
    for i in range(start_index, end_index, batch_size):
      self.load_hackernews_posts_batch(i, batch_size, 10)
  
  def load_hackernews_contents_range(self, start_index, end_index, batch_size=20, parallelism=20,  overwrite=False):
    if not overwrite:
      latest_index = self.db.get_max_content_id()
      start_index = latest_index + 1 if latest_index is not None else start_index
    while start_index < end_index:
      fetched_post_ids = self.load_hackernews_contents_batch(start_index, batch_size, parallelism)
      fetched_post_ids.sort()
      start_index = fetched_post_ids[-1] + 1

  def load_hackernews_posts_ids(self, post_ids):
    print(f"Loading posts {post_ids}")
    posts = []

    for id in post_ids:
      post = self.fetch_hn_post(str(id))
      if post is not None:
        posts.append(post)  
    
    self.db.insert_posts(posts)

  def load_hackernews_posts_batch(self, start_index, batch_size, parallelism=1):
    print(f"Loading {batch_size} posts beginning with {start_index}")
    posts = []
    ids = list(range(start_index, start_index + batch_size))

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
      results = executor.map(self.fetch_hn_post, map(str, ids))
    
    for result in results:
      if result is not None:
        posts.append(result)

    self.db.insert_posts(posts)
  
  def load_hackernews_contents_batch(self, start_index, batch_size, parallelism):
    print(f"Loading {batch_size} post contents beginning with {start_index}")
    return self.load_hackernews_contents_ids(start_index, batch_size, parallelism)

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

  def load_hackernews_contents_ids(self, start_index, batch_size, parallelism):
    posts = self.db.get_posts_from_id(start_index, batch_size)
    attributes = self.fetch_hn_content_urls_article3k(posts, parallelism)
    self.db.insert_doc_attributes(attributes, "hn_content")
    return [post['id'] for post in posts]

  def fetch_hn_content_urls_article3k(self, posts, parallelism=10):
    def fetch_article_content(post):
        try:
          print(f"Fetching content for {post}")
          article = Article(post['url'])
          article.download()
          article.parse()
          print(f"Got content for {post['id']}, length {len(article.text)}")
          return {
              "post_id": post['id'],
              "value": article.text
          }
        except Exception as e:
          print(f"Error fetching content for {post}: {e}")
          return None

    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        post_contents = list(executor.map(fetch_article_content, posts))

    return [post_content for post_content in post_contents if post_content is not None]

  def fetch_hn_content_urls_selenium(self, posts):
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

  def fetch_hn_content_selenium(self, driver, post_url):
    driver.get(post_url)
    driver.implicitly_wait(10)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    text = soup.get_text()
    return re.sub('(\s|\\\\n)+', ' ', text)
