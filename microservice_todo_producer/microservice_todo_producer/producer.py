from aiokafka import AIOKafkaProducer


async def get_producer():
    aio_producer = AIOKafkaProducer(bootstrap_servers="broker:19092")
    await aio_producer.start()
    try:
        yield aio_producer
    finally:
        await aio_producer.stop()