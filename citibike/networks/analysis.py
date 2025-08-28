from google.cloud import bigquery
from citibike.database.bigquery import initialize_bigquery_client
import pandas as pd
import networkx as nx


class CommuterNetworkAnalyzer:
    def __init__(self, config):
        self.client: bigquery.Client  = initialize_bigquery_client(config)
        self.project_id = config.get('GCP_PROJECT_ID')
        self.dataset = config.get('BQ_DATASET')
        self.SUPER_SOURCE = "super_source"
        self.SUPER_SINK = "super_sink"
    
    def _table_ref(self, table_name: str) -> str:
        """Construct a table id reference to a BigQuery table"""
        return f"{self.project_id}.{self.dataset}.{table_name}"
    
    def extract_network_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Queries the node and edge tables from BigQuery"""
        nodes_query = f"SELECT * FROM {self._table_ref('gold_commuter_hubs')}"
        edges_query = f"SELECT * FROM {self._table_ref('gold_commuter_edges')}"
        
        return (
            self.client.query(nodes_query).to_dataframe(),
            self.client.query(edges_query).to_dataframe()
        )
    
    def _build_graph(self, hubs_df: pd.DataFrame, edges_df: pd.DataFrame) ->nx.DiGraph:
        """Build directed graph of stations and edges"""
        G = nx.DiGraph()

        # Add nodes with station attributes
        for _, hub in hubs_df.iterrows():
            G.add_node(hub["station_id"],
                       name=hub["name"],
                       borough=hub["borough"],
                       capacity=hub["capacity"],
                       lat=hub["lat"],
                       lon=hub["lon"]
                       )
        
        # add edges with weights
        for _, edge in edges_df.iterrows():
            edge_attrs = {
                "distance_meters": edge["distance_meters"],
                "num_trips": edge["num_trips"],
                "avg_duration": edge["avg_duration"],
            }
            G.add_edge(edge["start_station_id"], edge["end_station_id"], **edge_attrs)
        
        return G
    
    def _calculate_centrality_metrics(self, station_graph: nx.DiGraph) -> dict:
        """Calculate centrality measures for network importance ranking"""

        return {
            "pagerank": nx.pagerank(station_graph, weight="num_trips"),
            "betweenness_centrality": nx.betweenness_centrality(station_graph, weight="num_trips"),
            "closeness_centrality": nx.closeness_centrality(station_graph, distance="avg_duration"),
            "degree_centrality": nx.degree_centrality(station_graph)
        }

    
    def _build_flow_network(self, hubs_df: pd.DataFrame, edges_df: pd.DataFrame) -> nx.DiGraph:
        """Build node-split flow network for capacity-constrained max flow"""
        G = nx.DiGraph()
        
        # Add node-split structure: each station becomes station_in -> station_out
        for _, hub in hubs_df.iterrows():
            station_id = hub['station_id']  # Using 'name' as the station identifier
            G.add_edge(f"{station_id}_in", f"{station_id}_out", 
                      capacity=hub['capacity'])
        
        # Add edges between stations with min-capacity constraint
        for _, edge in edges_df.iterrows():
            start_station = edge['start_station_id'] 
            end_station = edge['end_station_id']
            
            # Get capacities for edge constraint calculation
            station_capacities = hubs_df.set_index('station_id')['capacity'].to_dict()
            edge_capacity = min(station_capacities[start_station], station_capacities[end_station])
            
            G.add_edge(f"{start_station}_out", f"{end_station}_in", 
                      capacity=edge_capacity)
        
        # Find sources and sinks
        # True sources: stations that only have outgoing edges
        pure_sources = hubs_df[
            (hubs_df["in_degree"] == 0) & (hubs_df["out_degree"] > 0)
            ]['station_id'].tolist()
        
        # True sinks: stations that only have incoming edges
        pure_sinks = hubs_df[
            (hubs_df["out_degree"] == 0) & (hubs_df["in_degree"] > 0)
            ]['station_id'].tolist()

        # Connect true source / sink nodes to single super source / sink
        

        # Add single super source connected to all pure sources
        for source_station in pure_sources:
            G.add_edge(self.SUPER_SOURCE, f"{source_station}_in", capacity=float('inf'))
        
        # Add single super sink connected from all pure sinks
        for sink_station in pure_sinks:
            G.add_edge(f"{sink_station}_out", self.SUPER_SINK, capacity=float('inf'))
        
        return G
        
    def run_analysis(self, hubs_df: pd.DataFrame, edges_df: pd.DataFrame) -> pd.DataFrame:
        """Run complete network flow analysis"""
        # Build the flow network
        G = self._build_flow_network(hubs_df, edges_df)

        # Run max flow analysis
        flow_value, flow_dict = nx.maximum_flow(G, self.SUPER_SOURCE, self.SUPER_SINK)

        # Find critical and bottleneck nodes
        critical_nodes = self._find_critical_nodes(G)
        bottleneck_nodes = self._find_bottleneck_nodes(G)

        # Find centrality measures
        station_graph = self._build_graph(hubs_df, edges_df)
        centrality_metrics = self._calculate_centrality_metrics(station_graph)

        # Format results as DataFrame for BigQuery
        return self._format_analysis_results(hubs_df, critical_nodes, bottleneck_nodes, centrality_metrics)
    
    def _find_critical_nodes(self, G: nx.DiGraph) -> list[str]:
        """
        Find all critical nodes using residual graph reachability analysis.
        Critical nodes are those whose capacity decrease would reduce max flow
        """
        # Step 1: Get residual graph after max flow
        residual = nx.algorithms.flow.edmonds_karp(G, self.SUPER_SOURCE, self.SUPER_SINK)

        # Step 2: Find reachable vertices (only edges with remaining cpacity)
        positive_residual = residual.edge_subgraph([
            (u, v) for u, v, data in residual.edges(data=True)
            if data["capacity"] > data["flow"]
        ])

        reachable = nx.descendants(positive_residual, self.SUPER_SOURCE)

        # Step 3: Identify critical nodes
        critical_nodes = set()
        original_nodes = set([node[:-3] for node in G.nodes() if node.endswith("_in")])

        for node in original_nodes:
            if node not in [self.SUPER_SOURCE, self.SUPER_SINK]:
                node_in = f"{node}_in"
                node_out = f"{node}_out"

                if node_in in reachable and node_out not in reachable:
                    critical_nodes.add(node)
        
        return sorted(critical_nodes)

    def _find_bottleneck_nodes(self, G: nx.DiGraph) -> list[str]:
        """
        Find all bottleneck nodes using forward/backward reachability analysis.
        Bottleneck nodes are those whose capacity increase would increase max flow.
        """

        # Step 1: Get residual graph after max flow
        residual: nx.DiGraph = nx.algorithms.flow.edmonds_karp(G, self.SUPER_SOURCE, self.SUPER_SINK)

        # Step 2: Find reachable vertices (only edges with remaining cpacity)
        positive_residual = residual.edge_subgraph([
            (u, v) for u, v, data in residual.edges(data=True)
            if data["capacity"] > data["flow"]
        ])

        # Step 3: Forward and backward reachability
        forward_reach = nx.descendants(positive_residual, self.SUPER_SOURCE)
        backward_reach = nx.ancestors(positive_residual, self.SUPER_SINK)

        # Step 4: Identify bottleneck nodes
        bottleneck_nodes = set()
        original_nodes = set([node[:-3] for node in G.nodes() if node.endswith("_in")])

        for node in original_nodes:
            if node not in [self.SUPER_SOURCE, self.SUPER_SINK]:
                node_in = f"{node}_in"
                node_out = f"{node}_out"

                if node_in in forward_reach and node_out in backward_reach:
                    bottleneck_nodes.add(node)
        
        return sorted(bottleneck_nodes)
    
    def _format_analysis_results(self, 
                                 hubs_df: pd.DataFrame, 
                                 critical_nodes: list[str], 
                                 bottleneck_nodes: list[str], 
                                 centrality_metrics: dict ) -> pd.DataFrame:
        
        # Filter out non-critical and non-bottleneck nodes
        df = hubs_df[
            (hubs_df["station_id"].isin(critical_nodes)) | (hubs_df["station_id"].isin(bottleneck_nodes))
            ].copy()
        
        # Enrich with node type
        df["is_critical"] = df["station_id"].isin(critical_nodes)
        df["is_bottleneck"] = df["station_id"].isin(bottleneck_nodes)

        # Enrich with centrality metrics
        df['pagerank_score'] = df['station_id'].map(centrality_metrics['pagerank'])
        df['betweenness_centrality'] = df['station_id'].map(centrality_metrics['betweenness_centrality'])
        df['closeness_centrality'] = df['station_id'].map(centrality_metrics['closeness_centrality'])
        df['degree_centrality'] = df['station_id'].map(centrality_metrics['degree_centrality'])

        return df
        
    def write_results_to_bq(self, results_df: pd.DataFrame, table_name: str):
        table_ref = self._table_ref(table_name)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Complete replacement
        )
        
        job = self.client.load_table_from_dataframe(
            results_df, table_ref, job_config=job_config
        )
        job.result()  # Wait for completion
