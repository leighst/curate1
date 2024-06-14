from .agent import agent_resource
from .article_resource import WebArticleClient
from .hn_resource import HNAPIClient

RESOURCES_LOCAL = {
  "hn_client": HNAPIClient(),
  "article_client": WebArticleClient(),
  "agent_client": agent_resource.OpenAIAgentClient(),
}
