from pydantic import BaseModel


class AnnotatedDoc(BaseModel):
  doc: str
  annotation: str