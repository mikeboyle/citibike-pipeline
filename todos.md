# REFACTOR TODOs

## BigQuery preparation
- Drop / recreate tables in dev with script:
    - raw_trips_legacy
    - raw_trips_legacy_staging
    - raw_trips_current
    - raw_trips_current_staging
    - raw_stations
    - raw_stations_staging
    - DO NOT replace gold tables
- Drop silver_trips in BigQuery

- Rename or somehow back up the gold tables
    ```sql
    -- One-time setup
    CREATE SCHEMA IF NOT EXISTS `your-project.gold_backup_20250903`;

    -- Backup existing gold tables
    CREATE OR REPLACE TABLE `citibike-pipeline.gold_backup_20250903.gold_station_performance_dashboard` 
    AS SELECT * FROM `citibike-pipeline.citibike_dev.gold_station_performance_dashboard`;

    CREATE OR REPLACE TABLE `citibike-pipeline.gold_backup_20250903.gold_commuter_edges` 
    AS SELECT * FROM `citibike-pipeline.citibike_dev.gold_commuter_edges`;

    CREATE OR REPLACE TABLE `citibike-pipeline.gold_backup_20250903.gold_commuter_hubs` 
    AS SELECT * FROM `citibike-pipeline.citibike_dev.gold_commuter_hubs`;

    -- Verify backups
    SELECT 'original' as source, COUNT(*) as station_perf_count FROM `citibike-pipeline.citibike_dev.gold_station_performance_dashboard`
    UNION ALL
    SELECT 'backup' as source, COUNT(*) as station_perf_count FROM `citibike-pipeline.gold_backup_20250903.gold_station_performance_dashboard`;
    ```

## Debugging
- Make sure 1000 rows limit for trips is set
- Rerun trips pipeline with limit of 1000 to debug
    - 2015-06
    - 2025-04

## Bootstrapping corrected data
- Comment out the 1000 rows restriction
- Drop / recreate same tables in dev
- Rerun trips pipeline
    - 2015-06
    - 2025-04
    - 2025-05
    - 2025-06
    - 2025-07

- rerun the station perf dashboard - compare to backed up gold table
- rerun the network analysis pipeline - compare to backed up gold table

## Moving forward
- Create a single pipeline script that runs all data for a given year month through to the 

