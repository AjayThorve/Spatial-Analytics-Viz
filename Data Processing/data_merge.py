import cudf
from constants import list_of_states

nodes_df = []
edges_df = []
DATA_DIR = "../data/"

# read all states' parquet files and load them as list[cudf.DataFrames]
for state in list_of_states:
    nodes_df.append(cudf.read_parquet(f"{DATA_DIR}{state}-nodes.parquet"))
    edges_df.append(cudf.read_parquet(f"{DATA_DIR}{state}-edges.parquet"))

# process nodes
# concat all states' dataframes
nodes = cudf.concat(nodes_df)
# delete the original list of dataframes to avoid GPU OOM issues
del(nodes_df)
nodes.to_parquet(f"{DATA_DIR}us-nodes.parquet")

# process edges
# concat all states' dataframes
edges = cudf.concat(edges_df)
# delete the original list of dataframes to avoid GPU OOM issues
del(edges_df)
edges.to_parquet(f"{DATA_DIR}us-edges.parquet")
