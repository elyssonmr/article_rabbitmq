import asyncio
import json
import random

from rabbit_client import RabbitMQClient


async def save_statement(message):
    data = json.loads(message.body.decode('utf-8'))
    print(
        f'Saving statement: {data['order']} with '
        f'price: {data['price']} and '
        f'priority {message.priority}'
    )
    await asyncio.sleep(random.randint(1, 5))
    print('Statement Saved')
    await message.ack()


async def main():
    client = RabbitMQClient()
    await client.start()

    queue = 'accountant_system'
    fanout_exchange = 'order_fanout'

    await client.bind_queues_to_fanout_exchange(
        [queue], fanout_exchange
    )

    await client.consume(queue, save_statement)


if __name__ == '__main__':
    asyncio.run(main())
