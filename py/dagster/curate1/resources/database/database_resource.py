from abc import ABC, abstractmethod
from typing import List, Optional

from dagster import ConfigurableResource, InitResourceContext

from .database import Database, Document, DocumentAttribute


class DatabaseResource(ConfigurableResource, ABC):
    @abstractmethod
    def insert_documents(self, documents: List[Document]) -> None:
        pass

    @abstractmethod
    def insert_document_attributes(self, document_attributes: List[DocumentAttribute]) -> None:
        pass


class SqliteDatabaseResource(DatabaseResource):
    db_path: str
    _database: Database

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._database = Database(context.resource_config["db_path"])

    def insert_documents(self, documents: List[Document]) -> None:
        self._database.insert_documents(documents)

    def insert_document_attributes(self, document_attributes: List[DocumentAttribute]) -> None:
        self._database.insert_document_attributes(document_attributes)
      
