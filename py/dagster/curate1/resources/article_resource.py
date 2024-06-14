from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

import tenacity
from dagster import ConfigurableResource
from newspaper import Article


class ArticleClient(ConfigurableResource, ABC):
    @abstractmethod
    def fetch_article_content_batch(self, urls: List[str]) -> List[Optional[str]]:
        pass


class WebArticleClient(ArticleClient):
    def fetch_article_content_batch(self, urls: List[str]) -> List[Optional[str]]:
        return fetch_article_content_batch(urls)

retry_strategy = (
  tenacity.retry_if_exception(lambda e: "503" in str(e) or "429" in str(e))
)

@tenacity.retry(retry=retry_strategy, wait=tenacity.wait_exponential(multiplier=2), stop=tenacity.stop_after_attempt(5))
def fetch_article_content(url):
    try:
        print(f"Fetching content for {url}")
        article = Article(url)
        article.download()
        article.parse()
        print(f"Got content for {url}, length {len(article.text)}")
        return article.text
    except Exception as e:
        print(f"Error fetching content for {url}: {e}")
        raise

def fetch_article_content_with_retry(url):
    try:
        return fetch_article_content(url)
    except Exception as e:
        print(f"Error fetching content for {url}: {e}")
        return None

def fetch_article_content_batch(urls, parallelism=10):
    with ThreadPoolExecutor(max_workers=parallelism) as executor:
        post_contents = list(executor.map(fetch_article_content_with_retry, urls))
    return post_contents
