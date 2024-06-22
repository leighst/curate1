from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Optional

from dagster import ConfigurableResource, InitResourceContext

from .database import Database, Document, DocumentAttribute


class DatabaseResource(ConfigurableResource[Any], ABC): # type: ignore
    @abstractmethod
    def insert_documents(self, documents: List[Document]) -> List[int]:
        pass

    @abstractmethod
    def insert_document_attributes(self, document_attributes: List[DocumentAttribute]) -> List[int]:
        pass

    @abstractmethod
    def delete_documents_partition(self, partition_start: datetime, partition_end: datetime):
        pass

    @abstractmethod
    def delete_document_attributes_partition(self, partition_start: datetime, partition_end: datetime):
        pass


class SqliteDatabaseResource(DatabaseResource):
    db_path: str
    _database: Optional[Database] = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._database = Database(context.resource_config["db_path"])

    def insert_documents(self, documents: List[Document]) -> List[int]:
        if self._database is None:
            raise ValueError("Database is not initialized.")
        return self._database.insert_documents(documents)

    def insert_document_attributes(self, document_attributes: List[DocumentAttribute]) -> List[int]:
        if self._database is None:
            raise ValueError("Database is not initialized.")
        return self._database.insert_document_attributes(document_attributes)
    
    def delete_documents_partition(self, partition_start: datetime, partition_end: datetime):
        if self._database is None:
            raise ValueError("Database is not initialized.")
        return self._database.delete_documents_partition(partition_start, partition_end)

    def delete_document_attributes_partition(self, partition_start: datetime, partition_end: datetime):
        if self._database is None:
            raise ValueError("Database is not initialized.")
        return self._database.delete_document_attributes_partition(partition_start, partition_end)
      
