import os
import sqlite3
from typing import List, Optional

from pydantic import BaseModel


class Document(BaseModel):
  id: Optional[int]
  title: str
  content: str
  source_url: str
  created_at: int

class DocumentAttribute(BaseModel):
  id: Optional[int]
  document_id: int
  value: str
  created_at: int


class Database:
  def __init__(self, db_path: str):
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
      CREATE TABLE IF NOT EXISTS document (
        id INTEGER PRIMARY KEY,
        title TEXT,
        content TEXT,
        source_url TEXT,
        created_at INTEGER
      )
    ''')

    self.cursor.execute('''
      CREATE TABLE IF NOT EXISTS document_attribute (
        id INTEGER PRIMARY KEY,
        document_id INTEGER,
        value JSONB,
        label TEXT,
        created_at INTEGER,
        FOREIGN KEY(document_id) REFERENCES document(id)
      )
    ''')
    self.conn.commit()
    print("New database created.")
    
  def insert_documents(self, documents: List[Document]):
    cursor = self.conn.cursor()
    for document in documents:
      print("Adding", document)
      if document is None:
        continue
      sql = f'''
        INSERT INTO document (id, title, content, source_url, created_at)
        VALUES ({document.id}, '{document.title}', '{document.content}', '{document.source_url}', {document.created_at})
      '''
      print("SQL to be executed:", sql)
      cursor.execute('''
        INSERT INTO document (id, title, content, source_url, created_at)
        VALUES (?, ?, ?, ?, ?)
      ''', (document.id, document.title, document.content, document.source_url, document.created_at))
    self.conn.commit()
    print(f"{len(documents)} documents inserted into the database.")
  
  def insert_document_attributes(self, document_attributes: List[DocumentAttribute]):
    cursor = self.conn.cursor()
    for document_attribute in document_attributes:
      print("Adding", document_attribute)
      if document_attribute is None:
        continue
      sql = f'''
        INSERT INTO document_attribute (id, document_id, value, label, created_at)
        VALUES ({document_attribute.id}, {document_attribute.document_id}, '{document_attribute.value}', '{document_attribute.label}', {document_attribute.created_at})
      '''
      print("SQL to be executed:", sql)
      cursor.execute('''
        INSERT INTO document_attribute (id, document_id, value, label, created_at)
        VALUES (?, ?, ?, ?, ?)
      ''', (document_attribute.id, document_attribute.document_id, document_attribute.value, document_attribute.label, document_attribute.created_at))
    self.conn.commit()
    print(f"{len(document_attributes)} document attributes inserted into the database.")

