import os
from abc import ABC, abstractmethod

from ..database.database import Database


class LlmResponseCache(ABC):
  @abstractmethod
  def get_llm_response(self, prompt: str, model: str):
    pass

  @abstractmethod
  def insert_llm_response(self, prompt: str, model: str, response: str):
    pass

class DbResponseCache(LlmResponseCache):
  def __init__(self, database: Database):
    self.database = database
  
  def get_llm_response(self, prompt: str, model: str):
    return self.database.get_llm_response(prompt, model)

  def insert_llm_response(self, prompt: str, model: str, response: str):
    self.database.insert_llm_response(prompt, model, response)

  @staticmethod
  def from_env() -> LlmResponseCache:
    db_path = os.getenv('SQLITE_DATABASE_PATH')
    if db_path is None:
        raise ValueError("SQLITE_DATABASE_PATH environment variable is not set.")
    database=Database(db_path=db_path)
    return DbResponseCache(database)