from typing import Optional
from sqlmodel import SQLModel , Field

class Todo(SQLModel , table = True):
    id : Optional[int] = Field(default=None , primary_key=True)
    content : str