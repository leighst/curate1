import os

from .agent import agent_resource
from .article_resource import WebArticleClient
from .database.database_resource import SqliteDatabaseResource
from .hn_resource import HNAPIClient

db_path = os.getenv('SQLITE_DATABASE_PATH')
if db_path is None:
    raise ValueError("SQLITE_DATABASE_PATH environment variable is not set.")

database_resource = SqliteDatabaseResource(db_path=db_path)

RESOURCES_LOCAL = {
  "hn_client": HNAPIClient(),
  "article_client": WebArticleClient(),
  "agent_client": agent_resource.OpenAIAgentClient(),
  "database_resource": database_resource,
}
