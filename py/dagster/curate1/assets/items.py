from dagster import AssetExecutionContext, Output, asset
from pandas import DataFrame

from curate1.partitions import hourly_partitions
from curate1.resources.hn_resource import HNClient

from .id_range_for_time import id_range_for_time

@asset(partitions_def=hourly_partitions)
def stories(context: AssetExecutionContext, hn_client: HNClient) -> Output[DataFrame]:
    """Items from the Hacker News API: each is a story or a comment on a story."""
    (start_id, end_id), item_range_metadata = id_range_for_time(context, hn_client)

    context.log.info(f"Downloading range {start_id} up to {end_id}: {end_id - start_id} items.")

    rows = []
    for item_id in range(start_id, end_id):
        rows.append(hn_client.fetch_item_by_id(item_id))
        if len(rows) % 100 == 0:
            context.log.info(f"Downloaded {len(rows)} items!")

    non_none_rows = [row for row in rows if row is not None]
    items = DataFrame(non_none_rows).drop_duplicates(subset=["id"])
    stories = items.where(items["type"] == "story")

    return Output(
        stories,
        metadata={
            "Non-empty items": len(non_none_rows),
            "Empty items": rows.count(None),
            **item_range_metadata,
        },
    )

@asset(partitions_def=hourly_partitions)
def hn_documents(context: AssetExecutionContext, stories: DataFrame) -> Output[DataFrame]:
    return Output(stories)