import json
from typing import Any, Dict, List

import pandas as pd
from curate1.partitions import hourly_partitions
from curate1.resources.agent.agent_resource import AgentClient
from curate1.resources.agent.model import AnnotatedDoc
from curate1.resources.article_resource import ArticleClient
from curate1.resources.database.database import Document, DocumentAttribute
from curate1.resources.database.database_resource import DatabaseResource
from curate1.resources.hn_resource import HNClient
from dagster import AssetExecutionContext, Output, asset
from pandas import DataFrame

from .id_range_for_time import id_range_for_time

schema = {
    "id": (pd.Int64Dtype(), 0),
    "time": (pd.Int64Dtype(), 0),
    "type": (pd.StringDtype(), ""),
    "by": (pd.StringDtype(), ""),
    "dead": (pd.BooleanDtype(), False),
    "deleted": (pd.BooleanDtype(), False),
    "text": (pd.StringDtype(), ""),
    "kids": (pd.StringDtype(), ""),
    "score": (pd.Float64Dtype(), 0),
    "title": (pd.StringDtype(), ""),
    "descendants": (pd.Float64Dtype(), 0),
    "url": (pd.StringDtype(), "")
}

@asset(partitions_def=hourly_partitions)
def stories(
    context: AssetExecutionContext, 
    hn_client: HNClient
) -> Output[DataFrame]:
    """Items from the Hacker News API: each is a story or a comment on a story."""
    (start_id, end_id), item_range_metadata = id_range_for_time(context, hn_client)

    context.log.info(f"Downloading range {start_id} up to {end_id}: {end_id - start_id} items.")

    rows: List[Dict[str, Any]] = []
    for item_id in range(start_id, end_id):
        item = hn_client.fetch_item_by_id(item_id)
        if item is not None:  # Check if item is not None
            if (item_id-start_id) % 100 == 0:
                context.log.info(f"Downloaded {item_id-start_id} items!")
            if ("type" in item and item["type"] == "story" and 
                "url" in item and item["url"] != ""):
                rows.append(item)

    df = DataFrame(rows)
    for column, dtype in schema.items():
        if column not in df:
            df[column] = dtype[1]
        df[column] = df[column].fillna(dtype[1]) # type: ignore
        df[column] = df[column].astype(dtype[0]) # type: ignore
    
    return Output(
        df,
        metadata={
            "Rows": len(rows),
            "Excluded items": end_id-start_id-len(rows),
            **item_range_metadata,
        },
    )

#TODO: change to document_content
@asset(partitions_def=hourly_partitions)
def hackernews_documents( 
    context: AssetExecutionContext, 
    stories: DataFrame, 
    article_client: ArticleClient
) -> Output[DataFrame]:
    stories["document_id"] = stories["id"]
    
    stories_with_url: DataFrame = stories[stories["url"] != ""]
    none_url: DataFrame = stories[stories["url"] == ""]
    story_urls: List[str] = stories_with_url["url"].tolist()

    context.log.info(f"Downloading {len(story_urls)} stories...")
    story_contents: List[str|None] = article_client.fetch_article_content_batch(story_urls)

    stories_with_content: DataFrame = stories_with_url.assign(contents=story_contents) # type: ignore
    stories_with_content["contents"] = stories_with_content["contents"].fillna("") # type: ignore
    with_content: DataFrame = stories_with_content[stories_with_content["contents"] != ""]
    none_content = stories_with_content[stories_with_content["contents"] == ""]
    return Output(
        with_content,
        metadata={
            "Stories with URLs": len(stories_with_url),
            "Stories without URLs": len(none_url),
            "Stories with content": len(with_content),
            "Stories without content": len(none_content),
            "Story URLs (first 10)": story_urls[:10],
        }
    )

# TODO: use dynamic partitions for these
@asset(partitions_def=hourly_partitions)
def relevance_filter_spec_iac(
    context: AssetExecutionContext, 
    hackernews_documents: DataFrame, 
    agent_client: AgentClient
) -> Output[DataFrame]:
    return relevance_filter_spec(
        context, hackernews_documents, "iac", agent_client)

@asset(partitions_def=hourly_partitions)
def relevance_filter_spec_coding_with_ai(
    context: AssetExecutionContext, 
    hackernews_documents: DataFrame, 
    agent_client: AgentClient
) -> Output[DataFrame]:
    return relevance_filter_spec(
        context, hackernews_documents, "coding-with-ai", agent_client)

# should return document_id, highly_relevant, reasoning, label, value
def relevance_filter_spec(
    context: AssetExecutionContext, 
    hackernews_documents: DataFrame, 
    spec_name: str,
    agent_client: AgentClient
) -> Output[DataFrame]:
    contents: List[str] = hackernews_documents["contents"].tolist()
    
    spec_file = f"agent/prompts/specs/{spec_name}.txt"
    
    context.log.info(f"Annotating {len(contents)} docs...")
    annotated_docs = agent_client.filter_spec_batch(
        spec_file,
        contents
    )

    json_annotations = [a.annotation if a is not None else '{}' for a in annotated_docs]  # Handle None in annotations
    annotations = [json.loads(a) for a in json_annotations]
    highly_relevant = [a["highly_relevant"] for a in annotations]
    reasoning = [a["reasoning"] for a in annotations]

    hackernews_documents["highly_relevant"] = highly_relevant
    hackernews_documents["reasoning"] = reasoning
    hackernews_documents["label"] = f"filter_spec_{spec_name}"
    hackernews_documents["value"] = annotations

    non_empty_annotations = [a for a in annotations if a != ""]
    empty_annotations = [a for a in annotations if a == ""]

    num_highly_relevant = len([h for h in highly_relevant if h])
    num_not_relevant = len(annotated_docs) - num_highly_relevant

    return Output(
        hackernews_documents,
        metadata={
            "Non-empty annotations": len(non_empty_annotations),
            "Empty annotations": len(empty_annotations),
            "Highly relevant": num_highly_relevant,
            "Not relevant": num_not_relevant,
        },
    )

@asset(partitions_def=hourly_partitions)
def high_relevance_iac(
    relevance_filter_spec_iac: DataFrame, 
) -> DataFrame:
    return relevance_filter_spec_iac[relevance_filter_spec_iac["highly_relevant"]] # type: ignore

@asset(partitions_def=hourly_partitions)
def high_relevance_coding_with_ai(
    relevance_filter_spec_coding_with_ai: DataFrame, 
) -> DataFrame:
    return relevance_filter_spec_coding_with_ai[relevance_filter_spec_coding_with_ai["highly_relevant"]] # type: ignore

@asset(partitions_def=hourly_partitions)
def summary_perspective_summarizer_iac(
    context: AssetExecutionContext, 
    high_relevance_iac: DataFrame, 
    agent_client: AgentClient
) -> Output[DataFrame]:
    return summary_perspective_summarizer(
        context, high_relevance_iac, "perspective_summarizer_iac", agent_client)

@asset(partitions_def=hourly_partitions)
def summary_perspective_summarizer_coding_with_ai(
    context: AssetExecutionContext, 
    high_relevance_coding_with_ai: DataFrame, 
    agent_client: AgentClient,
) -> Output[DataFrame]:
    return summary_perspective_summarizer(
        context, high_relevance_coding_with_ai, "perspective_summarizer_coding_with_ai",  agent_client)
    
# should return document_id, summary, reasoning, label, value
def summary_perspective_summarizer(
    context: AssetExecutionContext, 
    relevance_filtered: DataFrame, 
    label: str,
    agent_client: AgentClient
) -> Output[DataFrame]:
    contents_with_reasoning: List[Tuple[str, str]] = list(zip(relevance_filtered['contents'], relevance_filtered['reasoning'])) # type: ignore
    
    context.log.info(f"Annotating {len(contents_with_reasoning)} docs...")
    annotated_docs: List[AnnotatedDoc|None] = agent_client.perspective_summarizer_batch(
        contents_with_reasoning
    )

    annotations = [json.loads(a.annotation) if a is not None else {} for a in annotated_docs]  # Handle None in annotations
    summary = [a["summary"] for a in annotations]
    reasoning = [a["reasoning"] for a in annotations]

    assert len(summary) == len(contents_with_reasoning)
    assert len(reasoning) == len(contents_with_reasoning)

    df = relevance_filtered.assign(summary=summary, reasoning=reasoning, value=annotations, label=label)
    return Output(
        df,
        metadata={
            "Input size": len(contents_with_reasoning),
            "Output size": len(summary),
        },
    )

@asset(partitions_def=hourly_partitions)
def attributes_data(
    context: AssetExecutionContext, 
    relevance_filter_spec_iac: DataFrame,
    relevance_filter_spec_coding_with_ai: DataFrame,
    summary_perspective_summarizer_iac: DataFrame,
    summary_perspective_summarizer_coding_with_ai: DataFrame
) -> Output[DataFrame]:
    context.log.info(f"Merging attributes data...")

    columns = ['document_id', 'time', 'value', 'label']

    all_data = pd.concat([
        relevance_filter_spec_iac[columns],
        relevance_filter_spec_coding_with_ai[columns],
        summary_perspective_summarizer_iac[columns],
        summary_perspective_summarizer_coding_with_ai[columns]
    ], ignore_index=True)

    return Output(
        all_data,
        metadata={
            "Relevance filter spec IAC": len(relevance_filter_spec_iac),
            "Relevance filter spec coding with AI": len(relevance_filter_spec_coding_with_ai),
            "Summary perspective summarizer IAC": len(summary_perspective_summarizer_iac),
            "Summary perspective summarizer coding with AI": len(summary_perspective_summarizer_coding_with_ai),
            "Merged rows": len(all_data)
        }
    )

@asset(partitions_def=hourly_partitions)
def sql_tables(
    context: AssetExecutionContext, 
    hackernews_documents: DataFrame, 
    attributes_data: DataFrame, 
    database_resource: DatabaseResource
) -> Output[DataFrame]:
    document_data = hackernews_documents

    # Delete existing document and attribute partitions
    start, end = context.partition_time_window
    database_resource.delete_documents_partition(start, end)
    database_resource.delete_document_attributes_partition(start, end)

    context.log.info(f"Saving {len(document_data)} documents to sql...")
    documents = [Document(
        id = None,
        title = document_data.iloc[i]["title"],
        content = document_data.iloc[i]["contents"],
        source_url = document_data.iloc[i]["url"],
        created_at = document_data.iloc[i]["time"]
    ) for i in range(len(document_data))]

    document_ids = database_resource.insert_documents(documents)
    document_data["document_id"] = document_ids   

    print(attributes_data.columns)

    context.log.info(f"Saving {len(attributes_data)} attributes to sql...")
    document_attributes = [DocumentAttribute(
        id = None,
        document_id = attributes_data.iloc[i]["document_id"],
        value = attributes_data.iloc[i]["value"],
        label = attributes_data.iloc[i]["label"],
        created_at = attributes_data.iloc[i]["time"] # inherit from document
    ) for i in range(len(attributes_data))]

    attribute_ids = database_resource.insert_document_attributes(document_attributes)
    attributes_data["attribute_id"] = attribute_ids

    return Output(
        attributes_data, # placeholder...
        metadata={
            "Documents inserted": len(document_ids),
            "Attributes inserted": len(attribute_ids),        
        }
    )
