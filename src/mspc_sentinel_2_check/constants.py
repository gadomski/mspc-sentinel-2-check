import pyarrow as pa

ACCOUNT_NAME = "sentinel2l2a01"
CONTAINER_NAME = "sentinel2-l2"
BATCH_SIZE = 1000
SCHEMA = pa.schema([("prefix", pa.string())])
YEAR_DEPTH = 3
MONTH_DEPTH = 4
SEMAPHORE_SIZE = 256
