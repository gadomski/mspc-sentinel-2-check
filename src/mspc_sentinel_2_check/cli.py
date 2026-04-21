import asyncio
from asyncio import Queue, Semaphore

import httpx
import typer
from obstore.store import AzureStore
from tqdm import tqdm

from mspc_sentinel_2_check.analyze import analyze as analyze_prefixes
from mspc_sentinel_2_check.constants import (
    ACCOUNT_NAME,
    CONTAINER_NAME,
    SEMAPHORE_SIZE,
)
from mspc_sentinel_2_check.prefixes import get_prefixes, write_prefixes

app = typer.Typer()


async def _run(year: str, month: str, output: str) -> None:
    sas_token = (
        httpx.get(
            f"https://planetarycomputer.microsoft.com/api/sas/v1/token/{ACCOUNT_NAME}/{CONTAINER_NAME}"
        )
        .raise_for_status()
        .json()["token"]
    )
    store = AzureStore(
        sas_key=sas_token,
        account_name=ACCOUNT_NAME,
        container_name=CONTAINER_NAME,
    )
    semaphore = Semaphore(SEMAPHORE_SIZE)
    queue: Queue[str | None] = Queue()
    with (
        tqdm(desc="listed", unit="prefix") as listed,
        tqdm(desc="written", unit="prefix") as written,
    ):
        writer_task = asyncio.create_task(write_prefixes(queue, output, written))
        await get_prefixes(store, "", year, month, semaphore, queue, listed)
        await queue.put(None)
        await writer_task


@app.command()
def prefixes(
    year: str = typer.Argument(..., help="Four-digit year, e.g. 2026"),
    month: str = typer.Argument(..., help="Two-digit month, e.g. 04"),
    output: str = typer.Option(
        None, "--output", "-o", help="Output parquet path (default: prefixes-<year>-<month>.parquet)"
    ),
) -> None:
    """List Sentinel-2 L2A .SAFE prefixes for a given year and month into a parquet file."""
    output_path = output or f"prefixes-{year}-{month}.parquet"
    asyncio.run(_run(year, month, output_path))


@app.command()
def analyze(
    path: str = typer.Argument(..., help="Path to a parquet file of .SAFE prefixes"),
) -> None:
    """Summarize a prefixes parquet file: total count, duplicates, and per-baseline counts."""
    result = analyze_prefixes(path)
    typer.echo(f"total: {result.total}")
    typer.echo(f"duplicates: {result.duplicates}")
    typer.echo(f"below baseline 0510: {result.below_baseline_0510}")
    typer.echo("by baseline:")
    for baseline, count in sorted(result.by_baseline.items()):
        typer.echo(f"  {baseline}: {count}")


if __name__ == "__main__":
    app()
