#!/usr/bin/env python

import asyncio
import logging

import anyio

from prefect.events.clients import PrefectEventSubscriber

# logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


async def main(timeout):
    with anyio.move_on_after(timeout):
        # async with PrefectEventSubscriber() as subscriber:
        #     subscriber.__aenter__()
        #     async for message in subscriber._websocket:
        #         message = orjson.loads(message)
        #         event: Event = Event.model_validate(message["event"])
        # async with get_events_subscriber() as subscriber:
        async with PrefectEventSubscriber(reconnection_attempts=10) as subscriber:
            async for event in subscriber:
                print(
                    '\n{"occurred": "'
                    + str(event.occurred)
                    + '", "event": "'
                    + event.event
                    + '", "id": "'
                    + str(event.id)
                    + '", "resource": {"prefect.resource.name": "'
                    + event.resource.name
                    + '", "prefect.resource.id": "'
                    + event.resource.id
                    + '"}}\n'
                )


if __name__ == "__main__":
    asyncio.run(main(600))
