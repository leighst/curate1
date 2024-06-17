import os

from .agent import agent_resource
from .article_resource import WebArticleClient
from .database.database import Database
from .database.database_resource import SqliteDatabaseResource
from .hn_resource import HNAPIClient

RESOURCES_LOCAL = {
  "hn_client": HNAPIClient(),
  "article_client": WebArticleClient(),
  "agent_client": agent_resource.OpenAIAgentClient(),
  "database_resource": SqliteDatabaseResource(db_path=os.getenv('SQLITE_DATABASE_PATH')),
}
