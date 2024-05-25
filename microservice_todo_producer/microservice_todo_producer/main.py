from typing import Annotated
from fastapi import Body, Depends, FastAPI
from aiokafka import AIOKafkaProducer
from sqlmodel import SQLModel, Session , create_engine, select
from microservice_todo_producer.model import Todo
import asyncio
from contextlib import asynccontextmanager
from microservice_todo_producer.settings import db_url
from aiokafka import AIOKafkaConsumer

connection_string = db_url.replace("postgresql" , "postgresql+psycopg2")

engine = create_engine(url=connection_string , echo=True)

def create_table():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session

@asynccontextmanager
async def lifespan(app:FastAPI):
    create_table()
    yield

app = FastAPI(title="Todo Producer" , lifespan=lifespan)

async def producer(message:str):
    aio_producer = AIOKafkaProducer(bootstrap_servers="broker:19092")
    print("Producer Starting....!")
    await aio_producer.start()
    print("Producer Started at [localhost:8000]...!")
    try:
        print(f"Producing message  [ message : {message}]....!")
        await aio_producer.send("todo" , message.encode("utf-8"))
    except:
        print(f"Error In Print message...!")
    finally:
        print("Producer is stopped....!")
        await aio_producer.stop()


@app.post("/add-todo" , tags=["Todo Routes"])
def add_todo(content = Body()):
    asyncio.run(producer(message=content))
    return content

@app.get('/get-todos' , tags=["Todo Routes"])
def get_route(session : Annotated[Session , Depends(get_session)]):
    result =  session.exec(select(Todo)).all()
    return result