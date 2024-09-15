import logging
import fsspec
import pyarrow.parquet as pq
from pyiceberg import catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import IdentityTransform

class DataLoader:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.fs = fsspec.filesystem("http")
        self.catalog = catalog.load_catalog("default")
        self.data_path = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'

    def get_data(self):
        """Loads data from the data path into a PyArrow table."""
        self.logger.info("Loading data from data path")
        return pq.read_table(self.data_path, filesystem=self.fs)

    def create_namespace(self, namespace_name):
        """Creates a namespace if it doesn't exist."""
        self.logger.info(f"Creating namespace {namespace_name}")
        self.catalog.create_namespace_if_not_exists(namespace_name)

    def create_taxi_table(self, table_name):
        """Creates a table if it doesn't exist, or gets a reference to it if it does exist."""
        if not self.catalog.table_exists(table_name):
            self.logger.info(f"Creating table {table_name}")
            df = self.get_data()
            self.catalog.create_table(
                table_name,
                schema=df.schema,
            )
            self.logger.info("Appending data to table")
            self.catalog.load_table(table_name).append(df)
            # appending 2 times to get multiple snapshots
            self.catalog.load_table(table_name).append(df)
        else:
            self.logger.info(f"Table {table_name} already exists")

    def print_scanned_rows(self, table_name):
        """Scans the table and prints the number of rows."""
        table = self.catalog.load_table(table_name)
        self.logger.info(f"Number of scanned rows: {len(table.scan().to_arrow())}")

    def create_bids_table(self, table_name):
        """Creates the bids table."""
        schema = Schema(
            NestedField(
                field_id=1,
                name="datetime",
                field_type=TimestampType(),
                required=True,
            ),
            NestedField(
                field_id=2,
                name="symbol",
                field_type=StringType(),
                required=True,
            ),
            NestedField(
                field_id=3,
                name="bid",
                field_type=FloatType(),
                required=False,
            ),
            NestedField(
                field_id=4,
                name="ask",
                field_type=DoubleType(),
                required=False,
            ),
            NestedField(
                field_id=5,
                name="details",
                field_type=StructType(
                    NestedField(
                        field_id=4,
                        name="created_by",
                        field_type=StringType(),
                        required=False,
                    ),
                ),
                required=False,
            ),
        )

        partition_spec = PartitionSpec(
            PartitionField(
                source_id=1,
                field_id=1000,
                transform=DayTransform(),
                name="datetime_day",
            )
        )

        sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))
        if not self.catalog.table_exists(table_name):
          self.catalog.create_table(
        		identifier=table_name,
        		schema=schema,
        		partition_spec=partition_spec,
        		sort_order=sort_order,
          )
          self.logger.info(f"Table created {table_name}")
        else:
          self.logger.info(f"Table {table_name} already existed")


def run():
    dl = DataLoader()
    namespace = 'default'
    taxi_table = 'taxi_dataset'
    bids_table = 'bids'
    dl.create_namespace(namespace)
    dl.create_taxi_table(f"{namespace}.{taxi_table}")
    dl.create_bids_table(f"{namespace}.{bids_table}")

run()

