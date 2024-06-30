import json
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from curate1.partitions import hourly_partitions
from curate1.resources.agent.agent_resource import AgentClient
from curate1.resources.agent.filter_spec import Relevance
from curate1.resources.agent.model import AnnotatedDoc
from curate1.resources.article_resource import ArticleClient
from curate1.resources.database.database import Document, DocumentAttribute
from curate1.resources.database.database_resource import DatabaseResource
from curate1.resources.hn_resource import HNClient
from dagster import AssetExecutionContext, Output, asset
from pandas import DataFrame, Series

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
        df[column] = df[column].fillna(dtype[1])
        df[column] = df[column].astype(dtype[0])
    
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

    stories_with_content: DataFrame = stories_with_url.assign(contents=story_contents)
    stories_with_content["contents"] = stories_with_content["contents"].fillna("")
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

@asset(partitions_def=hourly_partitions)
def candidate_docs_iac(
    context: AssetExecutionContext, 
    hackernews_documents: DataFrame, 
) -> Output[Optional[DataFrame]]:
    return keyword_filter_router(
        context, hackernews_documents, "iac")

@asset(partitions_def=hourly_partitions)
def candidate_docs_coding_with_ai(
    context: AssetExecutionContext, 
    hackernews_documents: DataFrame, 
) -> Output[Optional[DataFrame]]:
    return keyword_filter_router(
        context, hackernews_documents, "coding-with-ai")

SPEC_FILTER_MAP = {
    "iac": ["terraform", "iac", "pulumi", "infrastructure", "cloudformation", "infrastructure as code", "tf"],
    "coding-with-ai": ["llms", "llm", "ai", "coding", "code", "devin", "codegen", "code generation", "developer productivity", "coding assistant", "copilot", "cursor"],
}

def keyword_filter_router(
    context: AssetExecutionContext, 
    hackernews_documents: DataFrame, 
    spec_name: str
) -> Output[Optional[DataFrame]]:
    if spec_name not in SPEC_FILTER_MAP:
        raise ValueError(f"Spec name '{spec_name}' is not defined in SPEC_FILTER_MAP.")
    keywords = SPEC_FILTER_MAP[spec_name]
    keyword_pattern = r'\b(?:' + '|'.join(keywords) + r')\b'
    
    def matches_any_keyword(row: Series) -> bool:
        return row.astype(str).str.contains(keyword_pattern, case=False, regex=True).any()
    
    filtered_df = hackernews_documents[hackernews_documents.apply(matches_any_keyword, axis=1)]
    return Output(
        filtered_df, 
        metadata={
            "Spec": spec_name,
            "Keywords": keywords,
            "Input size": len(hackernews_documents),
            "Output size": len(filtered_df),
        }
    )

# TODO: use dynamic partitions for these
@asset(partitions_def=hourly_partitions)
def label_maybe_relevant_iac(
    context: AssetExecutionContext, 
    candidate_docs_iac: DataFrame, 
    agent_client: AgentClient
) -> Output[Optional[DataFrame]]:
    return relevance_filter_spec(
        context, candidate_docs_iac, "iac", Relevance.MAYBE_RELEVANT, agent_client)
    
@asset(partitions_def=hourly_partitions)
def label_maybe_relevant_coding_with_ai(
    context: AssetExecutionContext, 
    candidate_docs_coding_with_ai: DataFrame, 
    agent_client: AgentClient
) -> Output[Optional[DataFrame]]:
    return relevance_filter_spec(
        context, candidate_docs_coding_with_ai, "coding-with-ai", Relevance.MAYBE_RELEVANT, agent_client)

# TODO: use dynamic partitions for these
@asset(partitions_def=hourly_partitions)
def label_highly_relevant_iac(
    context: AssetExecutionContext, 
    maybe_relevant_iac: DataFrame, 
    agent_client: AgentClient
) -> Output[Optional[DataFrame]]:
    return relevance_filter_spec(
        context, maybe_relevant_iac, "iac", Relevance.HIGHLY_RELEVANT, agent_client)

@asset(partitions_def=hourly_partitions)
def label_highly_relevant_coding_with_ai(
    context: AssetExecutionContext, 
    maybe_relevant_coding_with_ai: DataFrame, 
    agent_client: AgentClient
) -> Output[Optional[DataFrame]]:
    return relevance_filter_spec(
        context, maybe_relevant_coding_with_ai, "coding-with-ai", Relevance.HIGHLY_RELEVANT, agent_client)

# TODO: use dynamic partitions for these
@asset(partitions_def=hourly_partitions)
def maybe_relevant_iac(
    context: AssetExecutionContext, 
    label_maybe_relevant_iac: DataFrame, 
    agent_client: AgentClient
) -> Output[Optional[DataFrame]]:
    return filter_relevance_labelled(
        context, label_maybe_relevant_iac)
    
@asset(partitions_def=hourly_partitions)
def maybe_relevant_coding_with_ai(
    context: AssetExecutionContext, 
    label_maybe_relevant_coding_with_ai: DataFrame, 
    agent_client: AgentClient
) -> Output[Optional[DataFrame]]:
    return filter_relevance_labelled(
        context, label_maybe_relevant_coding_with_ai)

# TODO: use dynamic partitions for these
@asset(partitions_def=hourly_partitions)
def highly_relevant_iac(
    context: AssetExecutionContext, 
    maybe_relevant_iac: DataFrame, 
) -> Output[Optional[DataFrame]]:
    return filter_relevance_labelled(
        context, maybe_relevant_iac)

@asset(partitions_def=hourly_partitions)
def highly_relevant_coding_with_ai(
    context: AssetExecutionContext, 
    maybe_relevant_coding_with_ai: DataFrame, 
) -> Output[Optional[DataFrame]]:
    return filter_relevance_labelled(
        context, maybe_relevant_coding_with_ai)

def filter_relevance_labelled(
    context: AssetExecutionContext, 
    relevance_labelled: DataFrame, 
) -> Output[Optional[DataFrame]]:
    relevance_filtered = relevance_labelled.loc[relevance_labelled["highly_relevant"]]
    return Output(
        relevance_filtered,
        metadata={
            "Input size": len(relevance_labelled),
            "Output size": len(relevance_filtered),
        },
    )

# should return document_id, highly_relevant, reasoning, label, value
def relevance_filter_spec(
    context: AssetExecutionContext, 
    hackernews_documents: DataFrame, 
    spec_name: str,
    relevance: Relevance,
    agent_client: AgentClient
) -> Output[Optional[DataFrame]]:
    contents: List[str] = hackernews_documents["contents"].tolist()

    context.log.info(f"Annotating {len(contents)} docs...")
    annotated_docs = agent_client.filter_spec_batch(
        spec_name,
        relevance,
        contents
    )

    json_annotations = [a.annotation if a is not None else '{}' for a in annotated_docs]  # Handle None in annotations

    annotations = [json.loads(a) for a in json_annotations]
    highly_relevant = [a["highly_relevant"] for a in annotations]
    reasoning = [a["reasoning"] for a in annotations]

    hackernews_documents["highly_relevant"] = highly_relevant
    hackernews_documents["reasoning"] = reasoning
    hackernews_documents["label"] = f"filter_spec_{spec_name}_{relevance.value}"
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
def summary_perspective_summarizer_iac(
    context: AssetExecutionContext, 
    highly_relevant_iac: DataFrame, 
    agent_client: AgentClient
) -> Output[DataFrame]:
    return summary_perspective_summarizer(
        context, highly_relevant_iac, "perspective_summarizer_iac", agent_client)

@asset(partitions_def=hourly_partitions)
def summary_perspective_summarizer_coding_with_ai(
    context: AssetExecutionContext, 
    highly_relevant_coding_with_ai: DataFrame, 
    agent_client: AgentClient,
) -> Output[DataFrame]:
    return summary_perspective_summarizer(
        context, highly_relevant_coding_with_ai, "perspective_summarizer_coding_with_ai",  agent_client)
    
# should return document_id, summary, reasoning, label, value
def summary_perspective_summarizer(
    context: AssetExecutionContext, 
    relevance_filtered: DataFrame, 
    label: str,
    agent_client: AgentClient
) -> Output[DataFrame]:
    contents_with_reasoning: List[Tuple[str, str]] = list(zip(relevance_filtered['contents'], relevance_filtered['reasoning']))
    
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
    label_maybe_relevant_iac: DataFrame,
    label_maybe_relevant_coding_with_ai: DataFrame,
    label_highly_relevant_iac: DataFrame,
    label_highly_relevant_coding_with_ai: DataFrame,
    summary_perspective_summarizer_iac: DataFrame,
    summary_perspective_summarizer_coding_with_ai: DataFrame
) -> Output[DataFrame]:
    context.log.info(f"Merging attributes data...")

    columns = ['document_id', 'time', 'value', 'label']

    all_data = pd.concat([
        label_maybe_relevant_iac[columns],
        label_maybe_relevant_coding_with_ai[columns],
        label_highly_relevant_iac[columns],
        label_highly_relevant_coding_with_ai[columns],
        summary_perspective_summarizer_iac[columns],
        summary_perspective_summarizer_coding_with_ai[columns]
    ], ignore_index=True)

    return Output(
        all_data,
        metadata={
            "Maybe relevant IAC": len(label_maybe_relevant_iac),
            "Maybe relevant coding with AI": len(label_maybe_relevant_coding_with_ai),
            "Highly relevant IAC": len(label_highly_relevant_iac),
            "Highly relevant coding with AI": len(label_highly_relevant_coding_with_ai),
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
) -> Output[None]:
    document_data = hackernews_documents

    # Delete existing document and attribute partitions
    start, end = context.partition_time_window
    deleted_documents = database_resource.delete_documents_partition(start, end)
    deleted_attributes = database_resource.delete_document_attributes_partition(start, end)

    context.log.info(f"Deleted {deleted_documents} documents and {deleted_attributes} attributes")

    context.log.info(f"Saving {len(document_data)} documents to sql...")
    documents = [Document(
        id = None,
        title = document_data.iloc[i]["title"],
        content = document_data.iloc[i]["contents"],
        source_url = document_data.iloc[i]["url"],
        created_at = document_data.iloc[i]["time"]
    ) for i in range(len(document_data))]

    document_ids = database_resource.insert_documents(documents)
    document_data["database_id"] = document_ids   

    context.log.info(f"Saving {len(attributes_data)} attributes to sql...")
    document_attributes = []
    
    for i in range(len(attributes_data)):
        doc_id = attributes_data.iloc[i]["document_id"]
        database_id = document_data.loc[document_data["document_id"] == doc_id]["database_id"].values[0]
        attr = DocumentAttribute(
            id = None,
            document_id = database_id,
            value = attributes_data.iloc[i]["value"],
            label = attributes_data.iloc[i]["label"],
            created_at = attributes_data.iloc[i]["time"] # inherit from document
        )
        document_attributes.append(attr)

    attribute_ids = database_resource.insert_document_attributes(document_attributes)
    attributes_data["attribute_id"] = attribute_ids

    return Output(
        None, 
        metadata={
            "Documents inserted": len(document_ids),
            "Attributes inserted": len(attribute_ids),
            "Documents deleted": deleted_documents,
            "Attributes deleted": deleted_attributes,
        }
    )
