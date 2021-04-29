# Data Processing

For generating the US-street map data, we use the Open Street Maps api to download nodes and edges for each state, and then merge them together. 

The graph data for us-streets(nodes and edges) is automatically downloaded and extracted on the first run.

Steps to reproduce dataset generation: 

> Warning: Downloading each state individually and processing it in memory could take a lot of hours. We do however save each state individually, so if the script terminates for some reason, rerunning it would continue data loading for only the remaining states.

```bash
# multiple instances of the script can also be run together, for parallel processing
python data_gen.py

# merge california regions(SE, NE, SW, NW) into a single file
bin/osmosis --read-pbf ../data/north-carolina-latest.osm.pbf --tee 4 --bounding-box left=-117 top=38 --write-pbf northcaliforniaSE.osm.pbf --bounding-box left=-117 bottom=38 --write-pbf northcaliforniaNE.osm.pbf --bounding-box right=-117 top=38 --write-pbf northcaliforniaSW.osm.pbf --bounding-box right=-117 bottom=38 --write-pbf northcaliforniaNW.osm.pbf

python data_merge.py
```

[data.ipynb](./data.ipynb) investigates the nodes and edges files.
