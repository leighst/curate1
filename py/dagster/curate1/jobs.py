from dagster import define_asset_job

from .partitions import hourly_partitions

curate1_job = define_asset_job(
  "curate1_job",
  partitions_def = hourly_partitions,
  config={
    "execution": {
      "config": {
        "multiprocess": {
          "max_concurrent": 1,      # limits concurrent assets to 1
        }
      }
    }
  },
)

curate1_processing_job = define_asset_job(
  "curate1_processing_job",
  partitions_def = hourly_partitions,
  selection=["candidate_docs_iac*", "candidate_docs_coding_with_ai*"],
  config={
    "execution": {
      "config": {
        "multiprocess": {
          "max_concurrent": 1,      # limits concurrent assets to 1
        }
      }
    }
  },
)