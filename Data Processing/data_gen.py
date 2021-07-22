import cudf
import pathlib
from pyrosm import get_data, OSM
DATA_DIR = "../data/"
from constants import regions


regions.reverse()

regions = regions[50:]

failed_regions = []
for region in regions:
    if not(pathlib.Path(f"{DATA_DIR}{region}-nodes.parquet").is_file() and pathlib.Path(f"{DATA_DIR}{region}-nodes.parquet").is_file()):
        print("starting processing for region "+region)
        try:
            fp = get_data(region, directory=DATA_DIR)
            osm = OSM(fp)
        except:
            print("failed for region", region)
            failed_regions.append(region)
            continue
        print("loading network")
        nodes, edges = osm.get_network(nodes=True, network_type="driving")
        print("loading network complete")
        edges_cudf = cudf.from_pandas(edges[['u', 'v', 'length']])
        edges_cudf.columns = ["src", "dst", "length"]
        nodes_cudf = cudf.from_pandas(nodes[['lon', 'lat', 'id']])
        nodes_cudf.columns = ["x", "y", "vertex"]
        print("saving as parquet files")
        nodes_cudf.to_parquet(DATA_DIR+f"{region}-nodes.parquet")
        edges_cudf.to_parquet(DATA_DIR+f"{region}-edges.parquet")
        print(f"processing for region {region} complete")
        del(nodes_cudf, edges_cudf, nodes, edges, fp, osm)
    else:
        print(f"region {region} already processed")


print("regions failed", failed_regions)


# bin/osmosis --read-pbf ../data/north-carolina-latest.osm.pbf --tee 4 --bounding-box left=-117 top=38 --write-pbf northcaliforniaSE.osm.pbf --bounding-box left=-117 bottom=38 --write-pbf northcaliforniaNE.osm.pbf --bounding-box right=-117 top=38 --write-pbf northcaliforniaSW.osm.pbf --bounding-box right=-117 bottom=38 --write-pbf northcaliforniaNW.osm.pbf
