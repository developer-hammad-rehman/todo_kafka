import json
from typing import Annotated
from fastapi import Body, Depends, FastAPI
from aiokafka import AIOKafkaProducer
from sqlmodel import SQLModel, Session, create_engine, select
from microservice_todo_producer.model import Todo
from contextlib import asynccontextmanager
from microservice_todo_producer.settings import db_url
from microservice_todo_producer.producer import get_producer

# from microservice_todo_producer import todo_pb2


connection_string = db_url.replace("postgresql", "postgresql+psycopg2")

engine = create_engine(url=connection_string, echo=True)


def create_table():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session


@asynccontextmanager
async def lifespan(app: FastAPI):
    create_table()
    yield


app = FastAPI(title="Todo Producer", lifespan=lifespan)


@app.post("/add-todo", tags=["Todo Routes"], response_model=Todo)
async def add_todo(
    producer: Annotated[AIOKafkaProducer, Depends(get_producer)], todo: Todo
):
    try:
        # todo_encode = todo_pb2.Todo(id=todo.id ,content=todo.content) # type: ignore
        # print(todo_encode.SerializeToString())
        await producer.send_and_wait(
            "todo", json.dumps(todo.model_dump()).encode("utf-8")
        )
    except Exception as e:
        print(e)
    return todo


@app.get("/get-todos", tags=["Todo Routes"])
def get_route(session: Annotated[Session, Depends(get_session)]):
    result = session.exec(select(Todo)).all()
    return result