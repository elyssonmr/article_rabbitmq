import asyncio
from typing import List

from aio_pika import ExchangeType, Message, connect_robust
from aio_pika.abc import AbstractChannel, AbstractExchange


class RabbitMQClient:
    def __init__(self, loop=None):
        if loop:
            self._loop = loop
        else:
            self._loop = asyncio.get_running_loop()

        self._conn = None
        self._publish_channel = None
    
    async def start(self):
        if not self._conn:
            self._conn = await connect_robust(
                'amqp://admin:admin@localhost:5672/',
                loop=self._loop
            )
            self._publish_channel = await self._conn.channel()

    async def stop(self):
        if not self._conn.is_closed:
            await self._conn.close()
            await self._conn.closed()

    async def publish_direct(self, message: str, routing_key: str, priority: int = 1):
        await self._publish_channel.declare_queue(
            routing_key,
            durable=True,
            arguments={"x-max-priority": 5}
        )
        message = Message(
            message.encode('utf-8'),
            priority=priority
        )
        await self._publish_channel.default_exchange.publish(
            message,
            routing_key=routing_key
        )

    async def publish_fanout(self, message: str, routing_key: str, priority: int = 1):
        exchange = await self.declare_fanout_exchange(
            routing_key, self._publish_channel
        )

        message = Message(
            message.encode('utf-8'),
            priority=priority
        )

        await exchange.publish(
            message, routing_key
        )

    async def declare_fanout_exchange(
        self,
        exchange_name: str,
        channel: AbstractChannel | None = None
    ) -> AbstractExchange | None:
        if channel:
            return await channel.declare_exchange(
                exchange_name,
                ExchangeType.FANOUT,
                durable=True
            )

        async with self._conn.channel() as channel:
            await channel.declare_exchange(
                exchange_name,
                ExchangeType.FANOUT,
                durable=True
            )


    async def bind_queues_to_fanout_exchange(
        self, queue_names: List[str], exchange_name: str
    ):
        async with self._conn.channel() as channel:
            fanout_exchange = await self.declare_fanout_exchange(
                exchange_name,
                channel
            )
            for queue_name in queue_names:
                queue = await channel.declare_queue(
                    queue_name,
                    durable=True,
                    arguments={'x-max-priority': 5}
                )
                await queue.bind(fanout_exchange)

    async def consume(self, queue, on_message, pre_fetch=1):
        async with self._conn.channel() as channel:
            await channel.set_qos(prefetch_count=pre_fetch)
            args = {
                'x-max-priority': 5
            }
            queue = await channel.declare_queue(
                queue,
                durable=True,
                arguments=args
            )
            await queue.consume(
                on_message,
                consumer_tag='consumer',
            )
            
            try:
                print('Start Consuming')
                await asyncio.Future()
            except asyncio.exceptions.CancelledError:
                print('Stopping worker')
                await self.stop()
