from os import listdir
from os.path import isfile, join
from pathlib import Path

bucket_name = "noaa-wcsd-zarr-pds"
ship_name = "Henry_B._Bigelow"
cruise_name = "HB1906"
sensor_name = "EK60"


def open_geojson(cruise):
    mypath = "./data/HB201906/"
    all_files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    all_evr_files = [i for i in all_files if Path(i).suffix == ".evr"]
    all_evr_files.sort()
    # all_evr_files = all_evr_files[:2]  # TODO: unset this!!!!!!!!!!
    # [1] open geojson
    # [2] read each record w properties
    # [3]
    # df = pd.DataFrame(pieces)
    # gps_gdf = gpd.GeoDataFrame(
    #     data=df[["id", "time", "ship", "cruise", "sensor", "label"]],
    #     geometry=df["geom"],
    #     crs="EPSG:4326",
    # )
    # print(gps_gdf)
    # #
    # # {'DXF': 'rw', 'CSV': 'raw', 'OpenFileGDB': 'raw', 'ESRIJSON': 'r', 'ESRI Shapefile': 'raw', 'FlatGeobuf': 'raw', 'GeoJSON': 'raw', 'GeoJSONSeq': 'raw', 'GPKG': 'raw', 'GML': 'rw', 'OGR_GMT': 'rw', 'GPX': 'rw', 'MapInfo File': 'raw', 'DGN': 'raw', 'S57': 'r', 'SQLite': 'raw', 'TopoJSON': 'r'}
    # if "GeoJSON" not in fiona.supported_drivers.keys():
    #     raise RuntimeError("Missing GeoJSON driver")
    #
    # gps_gdf.set_index("id", inplace=True)
    #
    # # write to file
    # gps_gdf.to_file(
    #     filename="point_dataset.geojson",
    #     driver="GeoJSON",
    #     engine="fiona",  # or "pyogrio"
    #     layer_options={"ID_GENERATE": "YES"},
    #     crs="EPSG:4326",
    #     id_generate=True,  # required for the feature click selection
    # )
    #
    # print(
    #     # TODO: above read into multiple geojson datasets, get ids of each, then write multiple files for multiple layers
    #     'Now run this: "tippecanoe --no-feature-limit -zg -o point_dataset.pmtiles -l biome point_dataset.geojson --force"'
    # )
    # # except Exception as err:
    # # raise RuntimeError(f"Problem parsing Zarr stores, {err}")
    # # I don't have the lat/lon information to draw here... need to query the zarr store...
    #
    # print("done")


# def open_zarr_store():
#     zarr_store = f"{cruise_name}.zarr"
#     store_path = f"s3://{bucket_name}/level_2/{ship_name}/{cruise_name}/{sensor_name}/{zarr_store}"
#     cruise = xr.open_dataset(
#         filename_or_obj=store_path,
#         engine="zarr",
#     )
#     print(cruise)
#     return cruise

if __name__ == "__main__":
    cruise = open_geojson()
    print(cruise)
    # open_evr_file(cruise)
