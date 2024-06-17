from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple

from dagster import ConfigurableResource

PARALLELISM = 4

class DatabaseResource(ConfigurableResource, ABC):
    @abstractmethod
    def filter_spec_batch(self, spec_file: str, contents: List[str]) -> Optional[str]:
        pass


class SqliteDatabaseResource(DatabaseResource):
    def filter_spec_batch(self, spec_file: str, contents: List[str]) -> List[Optional[AnnotatedDoc]]:
        
