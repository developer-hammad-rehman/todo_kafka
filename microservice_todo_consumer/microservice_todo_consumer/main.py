import json
from fastapi import Depends, FastAPI
from aiokafka import  AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import TopicAuthorizationFailedError , AuthenticationFailedError , TopicAlreadyExistsError ,  KafkaConnectionError
from aiokafka.admin import AIOKafkaAdminClient , NewTopic
from contextlib import asynccontextmanager
from sqlmodel import SQLModel, Session, create_engine
from microservice_todo_consumer.settings import data_base_url
from microservice_todo_consumer.models import Todo
import asyncio



# db_url = data_base_url.replace("postgresql" , "postgresql+psycopg2")

# def create_table():
#     SQLModel.metadata.create_all(engine)

# engine = create_engine(db_url , echo=True)


# def add_data(content:str):
#     data = Todo(content=content)
#     session = Session(engine)
#     session.add(data)
#     session.commit()
#     session.refresh(data)
#     session.close()

# async def admin():
#     admin_kafka = AIOKafkaAdminClient(bootstrap_servers="broker:19092")
#     print("Kafka Connecting...")
#     await admin_kafka.start() # type: ignore
#     print("Kakfa Connected...")
#     try:
#         print("Creating Topic...")
#         new_topic = NewTopic(name="todo" , num_partitions=1 , replication_factor=1)
#         await admin_kafka.create_topics(new_topics=[new_topic])
#         print("Topic Created...")
#     except (TopicAuthorizationFailedError , AuthenticationFailedError , TopicAlreadyExistsError , Exception) as e :
#         print("Error : Connection....")
#         print(str(e))
#     finally:
#         await admin_kafka.close()


async def consumer():
    consumer_kafka = AIOKafkaConsumer("todo"  , bootstrap_servers="broker:19092" , auto_offset_reset="earliest" , group_id="todo_main")
    print("Creating Kafka Consumer...")
    await consumer_kafka.start()
    print("Topic Created SucessFully")
    try:
        async for msg in consumer_kafka:
            print("Listening...")
            # value  = bytes(msg.value) # type: ignore
            # add_data(content = json.loads(value.decode("utf-8")))
            print(msg.value)
    finally:
        await consumer_kafka.stop() # type: ignore

@asynccontextmanager
async def lifespan(app:FastAPI):
    # create_table()
    task = asyncio.create_task(consumer())
    yield



app = FastAPI(lifespan=lifespan , title="Consumer Todo")


@app.get('/')
def root_route():
    return {"message" :"Consumer Server.."}