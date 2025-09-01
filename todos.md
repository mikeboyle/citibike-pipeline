# REFACTOR TODOs

## BigQuery preparation
- Drop / recreate tables in dev
    - raw_trips_legacy
    - raw_trips_legacy_staging
    - raw_trips_current
    - raw_trips_current_staging
    - raw_stations
    - raw_stations_staging
    - silver_stations

- Rename or somehow back up the gold tables

## Debugging
- Make sure 1000 rows limit for trips is set
- Rerun +silver_stations with limit of 1000 to debug
    - 2015-06
    - 2025-04

- If no errors, rerun the gold models to debug:
    - station perf dashboard
    - network analysis pipeline

- Make sure the aggregates and other results make sense (given the limited data)

## Bootstrapping corrected data
- Remove the 100 rows restriction
- Drop / recreate same tables in dev
- Rerun +silver_stations
    - 2015-06
    - 2025-04
    - 2025-05
    - 2025-06
    - 2025-07

- rerun the station perf dashboard - compare to backed up gold table
- rerun the network analysis pipeline - compare to backed up gold table

## Moving forward
- Create a single pipeline script that runs all data for a given year month through to the 

