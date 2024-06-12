from dagster import DailyPartitionsDefinition, define_asset_job

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