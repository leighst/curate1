import os
import sqlite3

class Database:
  def __init__(self, db_path):
      self.db_path = db_path
      self.conn = sqlite3.connect(db_path)
      self.cursor = self.conn.cursor()

  def recreate_db(self):
      print("Recreating database...")
      
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
      self.conn.commit()
      self.conn.close()
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
      self.conn.close()
      print(f"{len(posts)} posts inserted into the database.")
