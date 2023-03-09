  -- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE
  `dez_dataset.fhv_tripdata_2019_external` OPTIONS ( format = 'CSV',
    uris = ['gs://dez-gcs/fhv_tripdata_2019-*.csv.gz'] );

  -- Create a bq table from external table
CREATE OR REPLACE TABLE
  dez_dataset.fhv_tripdata_2019_bq AS
SELECT
  *
FROM
  dez_dataset.fhv_tripdata_2019_external;

  -- q1
SELECT
  COUNT(*)
FROM
  dez_dataset.fhv_tripdata_2019_external;

SELECT
  COUNT(*)
FROM
  dez_dataset.fhv_tripdata_2019_bq;

  -- q2
SELECT
  COUNT(DISTINCT affiliated_base_number)
FROM
  dez_dataset.fhv_tripdata_2019_external;

SELECT
  COUNT(DISTINCT affiliated_base_number)
FROM
  dez_dataset.fhv_tripdata_2019_bq;

  -- q3
SELECT
  COUNT(*)
FROM
  dez_dataset.fhv_tripdata_2019_bq
WHERE
  PUlocationID IS NULL
  AND DOlocationID IS NULL;

  --q5
  -- Creating a partition and cluster table
CREATE OR REPLACE TABLE
  dez_dataset.fhv_tripdata_2019_partitoned_clustered
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY
  affiliated_base_number AS
SELECT
  *
FROM
  dez_dataset.fhv_tripdata_2019_bq;

  -- query perf on bq table, processes 647.87 MB
SELECT
  DISTINCT affiliated_base_number
FROM
  dez_dataset.fhv_tripdata_2019_bq
WHERE
  pickup_datetime BETWEEN '2019-03-01'
  AND '2019-03-31';

  -- query perf on partition and cluster table, processes 23.05 MB
SELECT
  DISTINCT affiliated_base_number
FROM
  dez_dataset.fhv_tripdata_2019_partitoned_clustered
WHERE
  pickup_datetime BETWEEN '2019-03-01'
  AND '2019-03-31';