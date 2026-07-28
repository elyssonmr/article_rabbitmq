import asyncio
from functools import partial
import json
import random

from rabbit_client import RabbitMQClient


async def prepare_snack(client, message):
    data = json.loads(message.body.decode('utf-8'))
    print(
        f'Preparing Snack: {data['order']} for {data['name']} '
        f'with priority {message.priority}'
    )
    await asyncio.sleep(random.randint(10, 20))
    print('Snack Done')
    queue = 'delivery'
    await client.publish_direct(message.body.decode('utf-8'), queue)
    print('Snack sent to delivery')
    await message.ack()


async def main():
    client = RabbitMQClient()
    await client.start()

    queue = 'kitchen'
    fanout_exchange = 'order_fanout'

    await client.bind_queues_to_fanout_exchange(
        [queue], fanout_exchange
    )

    consume_func = partial(prepare_snack, client)
    await client.consume(queue, consume_func)


if __name__ == '__main__':
    asyncio.run(main())
