from curate1.partitions import hourly_partitions
from curate1.resources.article_resource import ArticleClient
from curate1.resources.hn_resource import HNClient
from dagster import AssetExecutionContext, Output, asset
from pandas import DataFrame

from .id_range_for_time import id_range_for_time

schema = {
    "id": (int, 0),
    "time": (int, 0),
    "type": (str, ""),
    "by": (str, ""),
    "dead": (bool, False),
    "deleted": (bool, False),
    "text": (str, ""),
    "kids": (str, ""),
    "score": (float, 0),
    "title": (str, ""),
    "descendants": (float, 0),
    "url": (str, "")
}

@asset(partitions_def=hourly_partitions)
def stories(
    context: AssetExecutionContext, 
    hn_client: HNClient
) -> Output[DataFrame]:
    """Items from the Hacker News API: each is a story or a comment on a story."""
    (start_id, end_id), item_range_metadata = id_range_for_time(context, hn_client)

    context.log.info(f"Downloading range {start_id} up to {end_id}: {end_id - start_id} items.")

    rows = []
    for item_id in range(start_id, end_id):
        item = hn_client.fetch_item_by_id(item_id)
        if (item_id-start_id) % 100 == 0:
            context.log.info(f"Downloaded {item_id-start_id} items!")
        if ("type" in item and item["type"] == "story" and 
            "url" in item and item["url"] != ""):
            rows.append(item)

    non_none_rows = [row for row in rows if row is not None]

    df = DataFrame.from_dict(non_none_rows)
    for column, dtype in schema.items():
        if column not in df:
            df[column] = dtype[1]
        # TODO: even this doesnt seem to fillna for the url field. ..
        df[column] = df[column].fillna(dtype[1]).astype(dtype[0])
    
    return Output(
        df,
        metadata={
            "Non-empty items": len(non_none_rows),
            "Empty items": rows.count(None),
            **item_range_metadata,
        },
    )

@asset(partitions_def=hourly_partitions)
def hackernews_documents(
    context: AssetExecutionContext, 
    stories: DataFrame, 
    article_client: ArticleClient
) -> Output[DataFrame]:
    stories_with_url = stories[stories["url"] != ""]
    none_url = stories[stories["url"] == ""]
    story_urls = stories_with_url["url"].tolist()

    context.log.info(f"Downloading {len(story_urls)} stories...")
    story_contents = article_client.fetch_article_content_batch(story_urls)

    stories_with_content = stories_with_url.assign(contents=story_contents)
    stories_with_content["contents"] = stories_with_content["contents"].fillna("")
    with_content = stories_with_content[stories_with_content["contents"] != ""]
    none_content = stories_with_content[stories_with_content["contents"] == ""]
    return Output(
        with_content,
        metadata={
            "Stories with URLs": len(stories_with_url),
            "Stories without URLs": len(none_url),
            "Stories with content": len(with_content),
            "Stories without content": len(none_content),
            "Story URLs (first 100)": story_urls[:100],
        }
    )

# TODO: use dynamic partitions for these

@asset(partitions_def=hourly_partitions)
def relevance_filter_spec_iac(
    context: AssetExecutionContext, 
    hackernews_documents: DataFrame, 
) -> Output[DataFrame]:
    pass

@asset(partitions_def=hourly_partitions)
def relevance_filter_spec_coding_with_ai(
    context: AssetExecutionContext, 
    hackernews_documents: DataFrame, 
) -> Output[DataFrame]:
    pass