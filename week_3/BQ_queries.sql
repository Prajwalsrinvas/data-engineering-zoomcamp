  -- Query public available table
SELECT
  station_id,
  name
FROM
  bigquery-public-data.new_york_citibike.citibike_stations
LIMIT
  100;

  -- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE
  `gcp_project_id.dez_dataset.external_yellow_tripdata` OPTIONS ( format = 'Parquet',
    uris = ['gs://dez-gcs/data/yellow/yellow_tripdata_2021-*.parquet'] );

  -- Check yellow trip data
SELECT
  COUNT(*)
FROM
  gcp_project_id.dez_dataset.external_yellow_tripdata;

  -- Create a non partitioned table from external table
CREATE OR REPLACE TABLE
  gcp_project_id.dez_dataset.yellow_tripdata_non_partitoned AS
SELECT
  *
FROM
  gcp_project_id.dez_dataset.external_yellow_tripdata;

  -- Create a partitioned table from external table
CREATE OR REPLACE TABLE
  gcp_project_id.dez_dataset.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT
  *
FROM
  gcp_project_id.dez_dataset.external_yellow_tripdata;

  -- Impact of partition
  -- Scanning 1.6GB of data
SELECT
  DISTINCT(VendorID)
FROM
  gcp_project_id.dez_dataset.yellow_tripdata_non_partitoned
WHERE
  DATE(tpep_pickup_datetime) BETWEEN '2021-01-01'
  AND '2021-04-30';

  -- Scanning ~106 MB of DATA
SELECT
  DISTINCT(VendorID)
FROM
  gcp_project_id.dez_dataset.yellow_tripdata_partitoned
WHERE
  DATE(tpep_pickup_datetime) BETWEEN '2021-01-01'
  AND '2021-04-30';

  -- Let's look into the partitons
SELECT
  table_name,
  partition_id,
  total_rows
FROM
  `dez_dataset.INFORMATION_SCHEMA.PARTITIONS`
WHERE
  table_name = 'yellow_tripdata_partitoned'
ORDER BY
  total_rows DESC;

  -- Creating a partition and cluster table
CREATE OR REPLACE TABLE
  gcp_project_id.dez_dataset.yellow_tripdata_partitoned_clustered
PARTITION BY
  DATE(tpep_pickup_datetime)
CLUSTER BY
  VendorID AS
SELECT
  *
FROM
  gcp_project_id.dez_dataset.external_yellow_tripdata;

  -- Query scans 1.1 GB
SELECT
  COUNT(*) AS trips
FROM
  gcp_project_id.dez_dataset.yellow_tripdata_partitoned
WHERE
  DATE(tpep_pickup_datetime) BETWEEN '2019-06-01'
  AND '2020-12-31'
  AND VendorID=1;
  
  -- Query scans 864.5 MB
SELECT
  COUNT(*) AS trips
FROM
  gcp_project_id.dez_dataset.yellow_tripdata_partitoned_clustered
WHERE
  DATE(tpep_pickup_datetime) BETWEEN '2019-06-01'
  AND '2020-12-31'
  AND VendorID=1;