import asyncio
import random

import anyio

from prefect import flow, task
from prefect.events.clients import get_events_subscriber
from prefect.events.filters import EventFilter, EventNameFilter
from prefect.states import State, StateType

MAX_SLEEP = 0.1
MAX_FLOWS = 10
MAX_TASKS = 10


@task
async def dummy_task(j: int):
    await asyncio.sleep(0.01 * random.randint(1, int(100 * MAX_SLEEP)))
    return j


@flow
async def child_flow(i: int, n_tasks: int):
    await asyncio.gather(
        *[
            dummy_task.with_options(name=f"dummy-task-{i}-{j}")(j)
            for j in range(n_tasks)
        ]
    )


@flow(log_prints=True)
async def parent_flow(n_subflows: int, n_tasks_per_subflow: int):
    event_filter = EventFilter(event=EventNameFilter(prefix=["prefect.flow-run"]))

    async with get_events_subscriber(filter=event_filter) as subscriber:
        await asyncio.gather(
            *[
                child_flow.with_options(name=f"child_flow-{i}")(i, n_tasks_per_subflow)
                for i in range(n_subflows)
            ]
        )

        data = {}
        with anyio.move_on_after(1.2 * MAX_SLEEP * MAX_FLOWS):
            async for event in subscriber:
                if not (state_type := event.resource.get("prefect.state-type")) or not (
                    name := event.resource.get("prefect.resource.name")
                ):
                    continue

                state_type = StateType(state_type)
                state = State(type=state_type)

                rna = name
                if rna not in data:
                    data[rna] = []
                data[rna].append(state.name)

        err = 0
        for k, v in data.items():
            if v != ["Pending", "Running", "Completed"]:
                err += 1
                print(k, v)

        print(len(data.keys()), err)

        return len(data.keys()), err


def test_concurrent_subflows():
    result = asyncio.run(parent_flow(MAX_FLOWS, MAX_TASKS))

    # Test passes if the flow completes without error
    assert result[0] > 0, "no events were captured through subscriber"
    assert result[1] == 0, "event sequence for flow not complete"


if __name__ == "__main__":
    # just run flow
    parent_flow.serve(name="testing_parent_flow")
