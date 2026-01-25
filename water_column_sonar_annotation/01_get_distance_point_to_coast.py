import geopandas as gpd
import shapely
from shapely.geometry import Point

"""
Gets the distance between a point and a coastline
https://www.kaggle.com/code/notcostheta/shortest-distance-to-a-coastline
https://www.naturalearthdata.com/downloads/50m-physical-vectors/50m-coastline/
"""


# nearest_points


def open_shape_file(
    latitude=44.2322653,
    longitude=-68.1454351,
):
    try:
        print("test")
        latitude = 42.4223343
        longitude = -63.8389551
        # evr_point = gpd.GeoSeries.from_xy(x=[longitude], y=[latitude], crs="EPSG:4326")
        evr_point = Point(longitude, latitude)
        #
        world_coastlines = gpd.read_file("ne_50m_coastline.shx")
        world_coastlines = world_coastlines.set_crs("EPSG:4326")
        first_linestring = world_coastlines.geometry[0]
        # country = world[world.name == "Australia"]
        # linedf = gpd.read_file(r'/home/bera/linewgs84.shp')
        # pointdf = gpd.read_file(r'/home/bera/pointwgs84.shp')
        # linedf.geometry.to_crs(epsg=32636).distance(pointdf.to_crs(epsg=32636))
        distance = first_linestring.distance(evr_point)
        distances = [
            world_coastlines.geometry[i].distance(evr_point)
            for i in range(len(world_coastlines.geometry))
        ]
        print(distance)
        print(distances)
    except Exception as e:
        print(f"Could not process cruise: {e}")
    print("done")


def check_wkt():
    try:
        # wkt_geometry_collection = "GEOMETRYCOLLECTION (LINESTRING (-69.963684 41.607228, -69.933472 41.806125, -69.993896 41.973785, -70.120239 42.079878), POINT (-69.367676 41.934977))"
        geometry_one = shapely.from_wkt(
            "LINESTRING (-69.963684 41.607228, -69.933472 41.806125, -69.993896 41.973785, -70.120239 42.079878)"
        )
        geometry_two = shapely.from_wkt("POINT (-69.367676 41.934977)")
        # evr_point = gpd.GeoSeries.from_xy(x=[longitude], y=[latitude], crs="EPSG:4326")
        # geom_col = shapely.from_wkt(wkt_geometry_collection)
        print(geometry_one)
        print(geometry_two)
        # https://stackoverflow.com/questions/70626218/how-to-find-the-nearest-linestring-to-a-point
        gdf_p = gpd.GeoDataFrame(geometry=[geometry_two], crs="epsg:4326")
        gdf_l = gpd.GeoDataFrame(geometry=[geometry_one], crs="epsg:4326")
        gdf_p = gdf_p.to_crs(gdf_p.estimate_utm_crs())
        gdf_l = gdf_l.to_crs(gdf_p.crs)
        df_n = gpd.sjoin_nearest(gdf_p, gdf_l).merge(
            gdf_l, left_on="index_right", right_index=True
        )
        df_n["distance_meters"] = df_n.apply(
            lambda r: r["geometry_x"].distance(r["geometry_y"]), axis=1
        )
        print(df_n)
        #
        # world_coastlines = gpd.read_file("ne_50m_coastline.shx")
        # world_coastlines = world_coastlines.set_crs("EPSG:4326")
        # first_linestring = world_coastlines.geometry[0]
        # country = world[world.name == "Australia"]
        # linedf = gpd.read_file(r'/home/bera/linewgs84.shp')
        # pointdf = gpd.read_file(r'/home/bera/pointwgs84.shp')
        # linedf.geometry.to_crs(epsg=32636).distance(pointdf.to_crs(epsg=32636))
        # distance = first_linestring.distance(evr_point)
        # distances = [
        #     world_coastlines.geometry[i].distance(evr_point)
        #     for i in range(len(world_coastlines.geometry))
        # ]
        # print(distance)
        # print(distances)
    except Exception as e:
        print(f"Could not process cruise: {e}")
    print("done")


if __name__ == "__main__":
    # open_shape_file()
    check_wkt()
