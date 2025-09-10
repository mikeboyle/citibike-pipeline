import os
from citibike.config import load_env_config
from citibike.dbt import run_dbt_command
from citibike.networks.analysis import CommuterNetworkAnalyzer


def run():
    # Load environment configuration
    env_name = os.environ.get('CITIBIKE_ENV', 'dev')
    load_env_config(env_name)
    
    # Dry run mode - validate configuration and exit
    if os.environ.get('CITIBIKE_DRY_RUN', '').lower() == 'true':
        print("✅ DRY RUN: Configuration loaded successfully")
        print(f"   Environment: {env_name}")
        print(f"   GCP_PROJECT_ID: {os.environ.get('GCP_PROJECT_ID', 'NOT SET')}")
        print(f"   BQ_DATASET: {os.environ.get('BQ_DATASET', 'NOT SET')}")
        print(f"   GOOGLE_APPLICATION_CREDENTIALS: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', 'NOT SET')}")
        
        # Test BigQuery connection
        try:
            from citibike.database.bigquery import initialize_bigquery_client
            initialize_bigquery_client(validate_connection=True)
        except Exception as e:
            print(f"❌ BigQuery connection validation failed: {e}")
            return
        
        print("⏹️  Stopping here - no data ingestion or dbt operations performed")
        return

    print("Stage 1: calculate edges for 90 day commuter network")

    edges_dbt_command = "dbt run --select gold_commuter_edges"
    run_dbt_command(edges_dbt_command.split())

    print("Stage 2: calculate nodes for 90 day commuter network")
    nodes_dbt_command = "dbt run --select gold_commuter_hubs"
    run_dbt_command(nodes_dbt_command.split())

    print("Stage 3: analyze commuter network")

    analyzer = CommuterNetworkAnalyzer()

    print("Extracting network data from BQ")
    hubs_df, edges_df = analyzer.extract_network_data()

    print(f"Loaded {len(hubs_df)} stations and {len(edges_df)} edges")

    print("Running network flow analysis")

    results_df = analyzer.run_analysis(hubs_df, edges_df)
    print(f"Found {len(results_df)} critical or bottleneck nodes")
    print(results_df.dtypes)
    print(results_df[['name', 'borough', 'is_critical', 'is_bottleneck', 'pagerank_score']].sort_values('pagerank_score', ascending=False).head(10))

    print("Stage 4: write analysis results to database")
    table_name = "gold_network_node_analysis"
    analyzer.write_results_to_bq(results_df, table_name)

    print("Network analysis pipeline complete!")

if __name__ == "__main__":
    run()