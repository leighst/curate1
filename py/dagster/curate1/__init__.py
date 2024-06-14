from dagster import Definitions, load_assets_from_modules

from .assets import items
from .jobs import curate1_job
from .resources import RESOURCES_LOCAL

all_assets = load_assets_from_modules([items])

defs = Definitions(
    assets=all_assets,
    resources=RESOURCES_LOCAL,
    jobs=[curate1_job],
)
