import json
import os
import sqlite3
from datetime import datetime
from typing import Any, List, Optional

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
  value: dict[str, Any]
  label: str
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

    self.cursor.execute('''
      CREATE TABLE IF NOT EXISTS llm_response_cache (
        id INTEGER PRIMARY KEY,
        model TEXT,
        prompt TEXT,
        response TEXT,
        created_at INTEGER
      )
    ''')
    self.conn.commit()
    print("New database created.")
    
  def delete_documents_partition(self, partition_start: datetime, partition_end: datetime):
    self.cursor.execute(f'''
      DELETE FROM document WHERE created_at >= ? AND created_at < ?
    ''', (partition_start.timestamp(), partition_end.timestamp()))
    self.conn.commit()
  
  def insert_documents(self, documents: List[Document]) -> List[int]:
    cursor = self.conn.cursor()
    inserted_ids: List[int] = []
    for document in documents:
      cursor.execute('''
        INSERT INTO document (title, content, source_url, created_at)
        VALUES (?, ?, ?, ?)
        RETURNING id
      ''', (document.title, document.content, document.source_url, document.created_at))
      inserted_id = cursor.fetchone()[0]
      inserted_ids.append(inserted_id)
    self.conn.commit()
    return inserted_ids

  def delete_document_attributes_partition(self, partition_start: datetime, partition_end: datetime):
    self.cursor.execute(f'''
      DELETE FROM document_attribute WHERE created_at >= ? AND created_at < ?
    ''', (partition_start.timestamp(), partition_end.timestamp()))
    self.conn.commit()

  def insert_document_attributes(self, document_attributes: List[DocumentAttribute]) -> List[int]:
    cursor = self.conn.cursor()
    inserted_ids: List[int] = []
    for document_attribute in document_attributes:
      json_value = json.dumps(document_attribute.value)
      cursor.execute('''
        INSERT INTO document_attribute (id, document_id, value, label, created_at)
        VALUES (?, ?, ?, ?, ?)
        RETURNING id
      ''', (document_attribute.id, document_attribute.document_id, json_value, document_attribute.label, document_attribute.created_at))
      inserted_id = cursor.fetchone()[0]
      inserted_ids.append(inserted_id)
    self.conn.commit()
    return inserted_ids

  def get_llm_response(self, prompt: str, model: str):
    self.cursor.execute('''
      SELECT response FROM llm_response_cache WHERE prompt = ? AND model = ?
    ''', (prompt, model))
    result = self.cursor.fetchone()
    if result:
      return result[0]
    return None
  
  def insert_llm_response(self, prompt: str, model: str, response: str):
    self.cursor.execute('''
      INSERT INTO llm_response_cache (model, prompt, response, created_at)
      VALUES (?, ?, ?, ?)
    ''', (model, prompt, response, datetime.now().timestamp()))
    self.conn.commit()
