import cudf
from constants import regions

nodes_df = []
edges_df = []
DATA_DIR = "../data/"

# read all regions' parquet files and load them as list[cudf.DataFrames]
for region in regions:
    nodes_df.append(cudf.read_parquet(f"{DATA_DIR}{region}-nodes.parquet"))
    edges_df.append(cudf.read_parquet(f"{DATA_DIR}{region}-edges.parquet"))

# process nodes
# concat each region's dataframes
nodes = cudf.concat(nodes_df)
# delete the original list of dataframes to avoid GPU OOM issues
del(nodes_df)
nodes.to_parquet(f"{DATA_DIR}eu-nodes.parquet")

# process edges
# concat each region's dataframes
edges = cudf.concat(edges_df)
# delete the original list of dataframes to avoid GPU OOM issues
del(edges_df)
edges.to_parquet(f"{DATA_DIR}eu-edges.parquet")
