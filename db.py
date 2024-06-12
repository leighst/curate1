import os
import sqlite3
import json
import time
from typing import List

class Database:
  def __init__(self, db_path):
    self.db_path = db_path
    self.conn = sqlite3.connect(db_path)
    self.cursor = self.conn.cursor()

  def recreate_db(self):
    print("Recreating database...")
    
    self.conn.close() # close the old connection
    if os.path.exists(self.db_path):
      os.remove(self.db_path)
      print("Existing database removed.")

    # Create a new database
    self.conn = sqlite3.connect(self.db_path)
    self.cursor = self.conn.cursor()
    self.cursor.execute('''
      CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY,
        title TEXT,
        url TEXT,
        score INTEGER,
        time INTEGER,
        by TEXT
      )
    ''')

    self.cursor.execute('''
      CREATE TABLE IF NOT EXISTS attributes (
        id INTEGER PRIMARY KEY,
        post_id INTEGER,
        value JSONB,
        label TEXT,
        time INTEGER,
        FOREIGN KEY(post_id) REFERENCES posts(id)
      )
    ''')
    self.conn.commit()
    print("New database created.")
    
  def insert_posts(self, posts):
    cursor = self.conn.cursor()
    for post in posts:
      print(post)
      if post is None:
        continue
      sql = f'''
        INSERT INTO posts (id, title, url, score, time, by)
        VALUES ({post['id']}, '{post['title']}', '{post['url']}', {post['score']}, {post['time']}, '{post['by']}')
      '''
      print("SQL to be executed:", sql)
      cursor.execute('''
        INSERT INTO posts (id, title, url, score, time, by)
        VALUES (?, ?, ?, ?, ?, ?)
      ''', (post['id'], post['title'], post['url'], post['score'], post['time'], post['by']))
    self.conn.commit()
    print(f"{len(posts)} posts inserted into the database.")

  def get_posts_by_ids(self, ids):
    cursor = self.conn.cursor()
    cursor.execute('''
      SELECT * FROM posts WHERE id IN ({})
    '''.format(','.join('?' for _ in ids)), ids)
    rows = cursor.fetchall()
    posts = []
    columns = [column[0] for column in cursor.description]
    for row in rows:
      post = dict(zip(columns, row))
      posts.append(post)
    return posts
  
  def get_posts_with_content_by_ids(self, ids):
    cursor = self.conn.cursor()
    cursor.execute('''
      SELECT posts.*, attributes.value AS content
      FROM posts
      JOIN attributes ON posts.id = attributes.post_id
      WHERE posts.id IN ({}) AND attributes.label = 'hn_content'
    '''.format(','.join('?' for _ in ids)), ids)
    rows = cursor.fetchall()
    posts = []
    columns = [column[0] for column in cursor.description]
    for row in rows:
      post = dict(zip(columns, row))
      posts.append(post)
    return posts
  
  def get_posts_from_id(self, start_id, count):
    cursor = self.conn.cursor()
    cursor.execute('''
      SELECT * FROM posts WHERE id >= ? ORDER BY id ASC LIMIT ?
    ''', (start_id, count))
    rows = cursor.fetchall()
    posts = []
    columns = [column[0] for column in cursor.description]
    for row in rows:
      post = dict(zip(columns, row))
      posts.append(post)
    return posts

  def insert_doc_attributes(self, attributes, label):
    cursor = self.conn.cursor()
    for post_attribute in attributes:
      post_id = post_attribute['post_id']
      value = post_attribute['value']
      cursor.execute('''
        INSERT INTO attributes (post_id, value, label, time)
        VALUES (?, ?, ?, ?)
      ''', (post_id, value, label, int(time.time())))
    self.conn.commit()
    print(f"Attributes with label '{label}' added for {len(attributes)} posts.")

  def get_post_with_content(self, post_id):
    cursor = self.conn.cursor()
    cursor.execute('''
      SELECT posts.*, attributes.value AS content
      FROM posts
      JOIN attributes ON posts.id = attributes.post_id
      WHERE posts.id = ?
    ''', (post_id,))
    row = cursor.fetchone()
    if row:
      columns = [column[0] for column in cursor.description]
      post_with_content = dict(zip(columns, row))
      return post_with_content
    else:
      print(f"No post found with ID {post_id}")
      return None
    
  def get_post_with_content_like(self, terms: List[str]):
    cursor = self.conn.cursor()
    query = "SELECT posts.*, attributes.value AS content FROM posts JOIN attributes ON posts.id = attributes.post_id WHERE " + " OR ".join([f"attributes.value LIKE ?" for _ in terms])
    cursor.execute(query, ['%' + term + '%' for term in terms])
    rows = cursor.fetchall()
    posts = []
    if rows:
      columns = [column[0] for column in cursor.description]
      for row in rows:
        post = dict(zip(columns, row))
        posts.append(post)
    return posts

  def get_posts_with_content_from_id(self, start_id, count, search_terms):
    like_terms = [f"%{s}%" for s in search_terms]
    
    print(f"search for posts from {start_id} with content like {like_terms}")

    cursor = self.conn.cursor()
    query = ('''
      SELECT posts.*, attributes.value AS content
      FROM posts
      JOIN attributes ON posts.id = attributes.post_id
      WHERE posts.id >= ? AND attributes.label = 'hn_content' AND (''' + 
      " OR ".join(["attributes.value LIKE ?" for _ in like_terms]) + 
      ") ORDER BY posts.id ASC LIMIT ?")
    print(query)
    cursor.execute(query, (start_id, *like_terms, count))
    rows = cursor.fetchall()
    posts = []
    columns = [column[0] for column in cursor.description]
    for row in rows:
      post = dict(zip(columns, row))
      posts.append(post)
    return posts

  def get_max_item_id(self):
    cursor = self.conn.cursor()
    cursor.execute('''
      SELECT MAX(id) FROM posts
    ''')
    max_id = cursor.fetchone()[0]
    return max_id

  def get_max_content_id(self):
    return self.get_max_attribute_id('hn_content')
  
  def get_max_attribute_id(self, label):
    cursor = self.conn.cursor()
    cursor.execute('''
      SELECT MAX(post_id) FROM attributes
      WHERE label = ?
    ''', (label,))
    max_id = cursor.fetchone()[0]
    return max_id

  def get_attributes(self, spec):
    cursor = self.conn.cursor()
    cursor.execute('''
      SELECT * FROM attributes WHERE label = ?
    ''', (spec,))
    rows = cursor.fetchall()
    attributes = []
    columns = [column[0] for column in cursor.description]
    for row in rows:
      attribute = dict(zip(columns, row))
      attributes.append(attribute)
    return attributes

