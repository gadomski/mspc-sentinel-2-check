import asyncio
from asyncio import Queue, Semaphore

import pyarrow as pa
from obstore.store import AzureStore
from pyarrow import parquet
from tqdm import tqdm

from mspc_sentinel_2_check.constants import (
    BATCH_SIZE,
    MONTH_DEPTH,
    SCHEMA,
    YEAR_DEPTH,
)


async def get_prefixes(
    store: AzureStore,
    prefix: str,
    year: str,
    month: str,
    semaphore: Semaphore,
    queue: Queue[str | None],
    listed: tqdm,
) -> None:
    """Recursively list blob prefixes under ``prefix``, filtered to ``year``/``month``.

    Completed ``.SAFE`` prefixes are pushed onto ``queue``. ``semaphore`` bounds
    the number of in-flight list requests and ``listed`` advances once per
    completed list call.
    """
    async with semaphore:
        common_prefixes = (await store.list_with_delimiter_async(prefix))[
            "common_prefixes"
        ]
        listed.update(1)

    tasks = []
    for common_prefix in common_prefixes:
        parts = common_prefix.rstrip("/").split("/")
        if len(parts) == YEAR_DEPTH + 1 and parts[YEAR_DEPTH] != year:
            continue
        if len(parts) == MONTH_DEPTH + 1 and parts[MONTH_DEPTH] != month:
            continue
        if common_prefix.endswith(".SAFE"):
            await queue.put(common_prefix)
        else:
            tasks.append(
                asyncio.create_task(
                    get_prefixes(
                        store, common_prefix, year, month, semaphore, queue, listed
                    )
                )
            )
    for task in tasks:
        await task


async def write_prefixes(
    queue: Queue[str | None], output_path: str, written: tqdm
) -> None:
    """Drain ``queue`` into a parquet file at ``output_path``, flushing in batches.

    Terminates when a ``None`` sentinel is received. ``written`` advances by the
    number of prefixes flushed per batch.
    """
    with parquet.ParquetWriter(output_path, SCHEMA) as writer:
        batch: list[str] = []
        while True:
            prefix = await queue.get()
            if prefix is None:
                if batch:
                    writer.write_table(pa.table({"prefix": batch}, schema=SCHEMA))
                    written.update(len(batch))
                return
            batch.append(prefix)
            if len(batch) >= BATCH_SIZE:
                writer.write_table(pa.table({"prefix": batch}, schema=SCHEMA))
                written.update(len(batch))
                batch = []
