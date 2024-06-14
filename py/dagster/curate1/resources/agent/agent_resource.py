from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

from dagster import ConfigurableResource

from .filter_spec import FilterSpec

parallelism = 4

class AgentClient(ConfigurableResource, ABC):
    @abstractmethod
    def filter_spec_batch(self, spec_file: str, contents: List[str]) -> Optional[str]:
        pass


class OpenAIAgentClient(AgentClient):
    def filter_spec_batch(self, spec_file: str, contents: List[str]) -> List[Optional[str]]:
        def annotate_post(content: str) -> Optional[str]:
            try:
                filter_spec = FilterSpec.from_env()
                docs = [content]
                annotation = filter_spec.apply(docs, spec_file)
                return annotation[0]
            except Exception as e:
                print(f"Error annotating {content[:20]}...: {e}")
                raise

        annotated_posts = []
        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            annotated_posts = list(executor.map(annotate_post, contents))
        
        return annotated_posts
