from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple

from dagster import ConfigurableResource, file_relative_path

from .filter_spec import FilterSpec, Relevance
from .model import AnnotatedDoc
from .perspective_summarizer import PerspectiveSummarizer

PARALLELISM = 4

class AgentClient(ConfigurableResource, ABC):
    @abstractmethod
    def filter_spec_batch(self, spec_file: str, relevance: Relevance, contents: List[str]) -> List[Optional[AnnotatedDoc]]:
        pass

    @abstractmethod
    def perspective_summarizer_batch(self, contents_with_reasoning: List[Tuple[str, str]]) -> List[Optional[AnnotatedDoc]]:
        pass


class OpenAIAgentClient(AgentClient):
    def filter_spec_batch(self, spec_name: str, relevance: Relevance, contents: List[str]) -> List[Optional[AnnotatedDoc]]:
        spec_file = file_relative_path(__file__, f"prompts/specs/{spec_name}.txt")

        def annotate_post(content: str) -> Optional[AnnotatedDoc]:
            try:
                filter_spec = FilterSpec.from_env(relevance)
                docs = [content]
                annotation = filter_spec.apply(docs, spec_file)
                # TOOD: switch to non array
                return annotation[0]
            except Exception as e:
                print(f"Error annotating {content[:20]}...: {e}")
                raise

        annotated_posts = []

        with ThreadPoolExecutor(max_workers=PARALLELISM) as executor:
            annotated_posts = list(executor.map(annotate_post, contents))
        
        return annotated_posts
    
    def perspective_summarizer_batch(self, contents_with_reasoning: List[Tuple[str, str]]) -> List[Optional[AnnotatedDoc]]:
        def annotate_post(pair: Tuple[str, str]) -> Optional[AnnotatedDoc]:
            contents, reasoning = pair
            try:
                summarizer = PerspectiveSummarizer.from_env()
                annotation = summarizer.apply(contents, reasoning)
                return annotation
            except Exception as e:
                print(f"Error annotating {contents[:20]}...: {e}")
                raise

        annotated_posts = []
        with ThreadPoolExecutor(max_workers=PARALLELISM) as executor:
            annotated_posts = list(executor.map(annotate_post, contents_with_reasoning))
        
        return annotated_posts
