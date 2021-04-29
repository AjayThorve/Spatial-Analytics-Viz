import math
import cuspatial
import cudf
import cugraph
from pyproj import Transformer
import geopandas as gpd
import dash_leaflet.express as dlx
from dask import delayed
import time
import numpy as np


def createCircleAroundWithRadius(lat, lon, radiusMiles):
    latArray = []
    lonArray = []

    for brng in range(0, 360):
        lat2, lon2 = getLocation(lat, lon, brng, radiusMiles)
        latArray.append(lat2)
        lonArray.append(lon2)

    return lonArray, latArray


def getLocation(lat1, lon1, brng, distanceMiles):
    lat1 = lat1 * math.pi / 180.0
    lon1 = lon1 * math.pi / 180.0
    # earth radius
    # R = 6378.1Km
    # R = ~ 3959 MilesR = 3959
    R = 3959

    distanceMiles = distanceMiles/R

    brng = (brng / 90) * math.pi / 2

    lat2 = math.asin(
        math.sin(lat1) * math.cos(distanceMiles) + math.cos(lat1) *
        math.sin(distanceMiles) * math.cos(brng)
    )

    lon2 = lon1 + math.atan2(
        math.sin(brng)*math.sin(distanceMiles) * math.cos(lat1),
        math.cos(distanceMiles)-math.sin(lat1)*math.sin(lat2)
    )

    lon2 = 180.0 * lon2 / math.pi
    lat2 = 180.0 * lat2 / math.pi

    return lat2, lon2


def get_updated_df_quadtree_pip(lat, lon, nodes_df):
    min_x, min_y, max_x, max_y = (nodes_df["x"].min(),
                                  nodes_df["y"].min(),
                                  nodes_df["x"].max(),
                                  nodes_df["y"].max())
    max_depth = 6
    min_size = 50
    scale = max(max_x - min_x, max_y - min_y) // (1 << max_depth)
    point_indices, quadtree = cuspatial.quadtree_on_points(
        nodes_df.x,
        nodes_df.y,
        min_x,
        max_x,
        min_y,
        max_y,
        scale,
        max_depth,
        min_size,
    )
    poly_offsets, ring_offsets = cudf.Series([0], index=["selection"]),[0]
    poly_bboxes = cuspatial.polygon_bounding_boxes(
        poly_offsets, ring_offsets, lat, lon,
    )
    intersections = cuspatial.join_quadtree_and_bounding_boxes(
        quadtree, poly_bboxes, min_x, max_x, min_y, max_y, scale, max_depth,
    )
    polygons_and_points = cuspatial.quadtree_point_in_polygon(
        intersections,
        quadtree,
        point_indices,
        nodes_df.x,
        nodes_df.y,
        poly_offsets,
        ring_offsets,
        lat,
        lon,
    )
    return nodes_df.loc[polygons_and_points.point_index]


def get_updated_df(lat, lon, nodes_df):
    results = cuspatial.point_in_polygon(
        nodes_df.x, nodes_df.y, cudf.Series([0], index=["selection"]),
        [0], lat, lon
    )
    return nodes_df[results.selection]


def get_updated_edges(nodes, edges):
    return edges.merge(nodes, left_on="src", right_on="vertex")[
        ['src', 'dst', 'length']
    ].merge(nodes, left_on="dst", right_on="vertex")[['src', 'dst', 'length']]


def get_shortest_paths(edges_df, point_of_interest):
    G_gpu = cugraph.Graph()
    G_gpu.from_cudf_edgelist(
        edges_df, source='src', destination='dst', edge_attr='time'
    )
    shortest_paths = cugraph.traversal.sssp(G_gpu, point_of_interest)
    shortest_paths = shortest_paths.drop('predecessor', axis=1)
    shortest_paths.columns = ['time', 'vertex']
    return shortest_paths


def get_nearest_node(gdf, point, x='x', y='y', osmid='osmid'):
    gdf = gdf
    gdf['point_y'] = point[0]
    gdf['point_x'] = point[1]
    gdf['distance'] = cuspatial.haversine_distance(
        gdf[y], gdf[x], gdf['point_y'], gdf['point_x']
    )
    mask = gdf['distance'] == gdf['distance'].min()
    nearest_node = gdf[mask][osmid].values[0]
    gdf.drop(['point_y', 'point_x', 'distance'], axis=1, inplace=True)
    return nearest_node


def get_polygons_for_travel_time(results, trip_times):
    if isinstance(trip_times, (int, float)):
        trip_times = [trip_times]

    for trip_time in trip_times:
        results['within_' + str(trip_time)] = 1.0 * (
            results['time'] < trip_time
        )

    # make the isochrone polygons
    isochrone_polys = []
    for trip_time in sorted(trip_times, reverse=True):
        mask = results['within_' + str(trip_time)] == 1
        subset = results[mask].to_pandas()
        node_points = gpd.points_from_xy(subset.x, subset.y)
        bounding_poly = gpd.GeoSeries(node_points).unary_union.convex_hull
        isochrone_polys.append(bounding_poly)

    return isochrone_polys


def query_census_dataset(polygons, census_data):
    final_polygon = polygons[0]
    for i in polygons:
        final_polygon = final_polygon.union(i)
    lat, lon = final_polygon.exterior.coords.xy
    transform_4326_to_3857 = Transformer.from_crs('epsg:4326', 'epsg:3857')
    lon, lat = transform_4326_to_3857.transform(lon, lat)
    results = cuspatial.point_in_polygon(
        census_data.x, census_data.y, cudf.Series([0], index=["selection"]),
        [0], lon, lat
    )
    return census_data[results.selection]


def get_data(df, color_prop="income"):
    if len(df) > 0:
        transform_3857_to_4326 = Transformer.from_crs('epsg:3857', 'epsg:4326')
        df['lat'], df['lon'] = transform_3857_to_4326.transform(
            df.x.to_array(), df.y.to_array()
        )
        # drop irrelevant columns
        df = df[['lat', 'lon', 'sex', 'education', 'income']]
        if isinstance(df, cudf.DataFrame):
            dicts = df.to_pandas().to_dict('rows')
        else:
            dicts = df.to_dict('rows')
        for item in dicts:
            # bind tooltip
            item["tooltip"] = "{:.1f}".format(item[color_prop])
            # bind popup
            item["popup"] = item["income"]
        # convert to geojson
        geojson = dlx.dicts_to_geojson(dicts, lat="lat", lon="lon")
        # convert to geobuf
        return geojson
    return ''


distanceInMiles = 30


def get_nearest_polygons_from_selected_point(
    point_lat, point_lon, average_speed, trip_time,
    nodes_df, edges_df, census_data
):
    times = [time.time()]

    lat, lon = createCircleAroundWithRadius(
        point_lat, point_lon, distanceInMiles
    )
    nodes = get_updated_df(lat, lon, nodes_df)

    edges = get_updated_edges(nodes, edges_df)
    times.append(time.time())

    # km per hour to m per minute
    meters_per_minute = (average_speed * 1000) / 60
    edges['time'] = edges['length'] / meters_per_minute

    point_of_interest = get_nearest_node(
        nodes, point=(point_lat, point_lon), x='x', y='y', osmid='vertex'
    )
    times.append(time.time())
    shortest_paths = get_shortest_paths(edges, point_of_interest)
    results = cudf.merge(shortest_paths, nodes[
        ['vertex', 'y', 'x']], on='vertex', how='inner'
    )
    times.append(time.time())
    polygons = get_polygons_for_travel_time(results, trip_time)
    d = gpd.geodataframe.from_shapely(polygons)
    polygon = gpd.GeoDataFrame(
        index=[i for i in range(len(d))], geometry=d).reset_index()
    times.append(time.time())

    times = np.diff(times)
    times = np.round(times, 4)

    del results, shortest_paths, edges, nodes
    return (
        polygon,
        delayed(query_census_dataset)(polygons, census_data).compute(),
        times
    )
