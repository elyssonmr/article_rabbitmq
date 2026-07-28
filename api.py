from decimal import Decimal
import random
import json

from typing import Annotated

from fastapi import Depends, FastAPI

from rabbit_client import RabbitMQClient

from pydantic import BaseModel


app = FastAPI()


async def get_rabbit_client():
    client = RabbitMQClient()
    await client.start()
    yield client
    await client.stop()


T_RabbitMQClient = Annotated[RabbitMQClient, Depends(get_rabbit_client)]


class OrderSchema(BaseModel):
    name: str
    order: str
    address: str


def random_price() -> Decimal:
    return Decimal(random.randint(500, 2500)).scaleb(-2)


@app.post('/order')
async def order(client_order: OrderSchema, rabbit_client: T_RabbitMQClient):
    # Save Order first
    message = client_order.model_dump(mode='json')
    # Retrieve price from database
    message['price'] = str(random_price())
    routing_key = 'order_fanout'

    await rabbit_client.publish_fanout(
        json.dumps(message),
        routing_key,
        priority=random.randint(1, 4),
    )
