import cudf
import pandas as pd
import cugraph
import cuspatial
import pathlib
from pyrosm import get_data, OSM
DATA_DIR = "../data/"
from constants import list_of_states


failed_states = []
for state in list_of_states:
    if not(pathlib.Path(f"{DATA_DIR}{state}-nodes.parquet").is_file() and pathlib.Path(f"{DATA_DIR}{state}-nodes.parquet").is_file()):
        print("starting processing for state "+state)
        try:
            fp = get_data(state, directory=DATA_DIR)
            osm = OSM(fp)
        except:
            print("failed for state", state)
            failed_states.append(state)
            continue
        print("loading network")
        nodes, edges = osm.get_network(nodes=True, network_type="driving")
        print("loading network complete")
        edges_cudf = cudf.from_pandas(edges[['u', 'v', 'length']])
        edges_cudf.columns = ["src", "dst", "length"]
        nodes_cudf = cudf.from_pandas(nodes[['lon', 'lat', 'id']])
        nodes_cudf.columns = ["x", "y", "vertex"]
        print("saving as parquet files")
        nodes_cudf.to_parquet(DATA_DIR+f"{state}-nodes.parquet")
        edges_cudf.to_parquet(DATA_DIR+f"{state}-edges.parquet")
        print(f"processing for state {state} complete")
        del(nodes_cudf, edges_cudf, nodes, edges, fp, osm)
    else:
        print(f"state {state} already processed")


print("states failed", failed_states)


# bin/osmosis --read-pbf ../data/north-carolina-latest.osm.pbf --tee 4 --bounding-box left=-117 top=38 --write-pbf northcaliforniaSE.osm.pbf --bounding-box left=-117 bottom=38 --write-pbf northcaliforniaNE.osm.pbf --bounding-box right=-117 top=38 --write-pbf northcaliforniaSW.osm.pbf --bounding-box right=-117 bottom=38 --write-pbf northcaliforniaNW.osm.pbf
