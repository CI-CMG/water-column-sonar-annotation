from os import listdir
from os.path import isfile, join
from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd
import xarray as xr
from shapely import Point

bucket_name = "noaa-wcsd-zarr-pds"
ship_name = "Henry_B._Bigelow"
cruise_name = "HB1906"
sensor_name = "EK60"


def open_evr_file(cruise):
    mypath = "./data/HB201906/"
    all_files = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    all_evr_files = [i for i in all_files if Path(i).suffix == ".evr"]
    all_evr_files.sort()
    all_evr_files = all_evr_files[:2]  # TODO: unset this!!!!!!!!!!

    for evr_file in all_evr_files:
        print(evr_file)
        with open(mypath + evr_file, "r") as file:
            lines = file.read()
        records = lines.split("\n\n")
        records = [i for i in records if i.startswith("13 ")]  # filter
        pieces = []
        for index, record in enumerate(records):
            print("_+_+_+_+ start new record _+_+_+")
            times = record.split(" ")[7:9]  # get the date/time
            converted_time = pd.to_datetime(
                f"{times[0]}T{times[1]}", format="%Y%m%dT%H%M%S%f"
            )
            record_split = [x for x in record.split("\n") if x]
            # bounding_box = record_split[0]  # TODO: get box
            polygon_label = record_split[-3]
            # polygon_vertices = record_split[-2]
            # all_vertices = list(
            #     zip(*[iter(polygon_vertices.split(" "))] * 3)
            # )  # date time depth
            # TODO: iterate vertices and format
            # for vertice in all_vertices:
            #    create time & depth annotation
            # combined_polygon = []
            latitude = cruise.latitude.sel(time=converted_time, method="nearest").values
            longitude = cruise.longitude.sel(
                time=converted_time, method="nearest"
            ).values
            # also need ping_time_index?
            print(
                f"time: {converted_time}, label: {polygon_label}, lat: {latitude}, lon: {longitude}"
            )
            geom = Point(longitude, latitude)
            pieces.append(
                {
                    "id": index,
                    "time": converted_time,
                    "ship": ship_name,
                    "cruise": cruise_name,
                    "sensor": sensor_name,
                    "label": polygon_label,
                    "geom": geom,
                }
            )
        df = pd.DataFrame(pieces)
        gps_gdf = gpd.GeoDataFrame(
            data=df[["id", "time", "ship", "cruise", "sensor", "label"]],
            geometry=df["geom"],
            crs="EPSG:4326",
        )
        print(gps_gdf)
        #
        # {'DXF': 'rw', 'CSV': 'raw', 'OpenFileGDB': 'raw', 'ESRIJSON': 'r', 'ESRI Shapefile': 'raw', 'FlatGeobuf': 'raw', 'GeoJSON': 'raw', 'GeoJSONSeq': 'raw', 'GPKG': 'raw', 'GML': 'rw', 'OGR_GMT': 'rw', 'GPX': 'rw', 'MapInfo File': 'raw', 'DGN': 'raw', 'S57': 'r', 'SQLite': 'raw', 'TopoJSON': 'r'}
        if "GeoJSON" not in fiona.supported_drivers.keys():
            raise RuntimeError("Missing GeoJSON driver")

        gps_gdf.set_index("id", inplace=True)

        # write to file
        gps_gdf.to_file(
            filename="point_dataset.geojson",
            driver="GeoJSON",
            engine="fiona",  # or "pyogrio"
            layer_options={"ID_GENERATE": "YES"},
            crs="EPSG:4326",
            id_generate=True,  # required for the feature click selection
        )

        print(
            'Now run this: "tippecanoe --no-feature-limit -zg -o point_dataset.pmtiles -l cruises point_dataset.geojson --force"'
        )
    # except Exception as err:
    # raise RuntimeError(f"Problem parsing Zarr stores, {err}")
    # I don't have the lat/lon information to draw here... need to query the zarr store...

    print("done")


def open_zarr_store():
    zarr_store = f"{cruise_name}.zarr"
    store_path = f"s3://{bucket_name}/level_2/{ship_name}/{cruise_name}/{sensor_name}/{zarr_store}"
    cruise = xr.open_dataset(
        filename_or_obj=store_path,
        engine="zarr",
    )
    print(cruise)
    return cruise


"""
13 12 1 0 2 -1 1 20190925 2053458953  9.2818 20190925 2054119318  11.5333
0
0
Unclassified regions
20190925 2053458953 9.6034489515 20190925 2053521545 11.1197829964 20190925 2054046730 11.5333286451 20190925 2054064248 11.5333286451 20190925 2054079263 11.4414296120 20190925 2054116810 10.8440858974 20190925 2054119318 10.4764897652 20190925 2054116810 9.6953479845 20190925 2054111800 9.4196508854 20190925 2054091775 9.2818023359 20190925 2054076760 9.2818023359 20190925 2053483995 9.5574994350 0
100

13 52 26 0 7 -1 1 20191016 1851472920  142.7737148547 20191016 1851573110  147.8358025219
0
10
School detected with:
Minimum data threshold:  -66.00
Maximum data threshold: (none)
Distance mode: GPS distance
Minimum total school height (meters):   4.00
Minimum candidate length (meters):   1.00
Minimum candidate height (meters):   2.00
Maximum vertical linking distance (meters):   2.00
Maximum horizontal linking distance (meters):  20.00
Minimum total school length (meters):   4.00
AH_School
20191016 1851472920 143.9418889317 20191016 1851472920 144.3312802907 20191016 1851482694 144.3312802907 20191016 1851482945 144.3361476827 20191016 1851482945 145.4945869758 20191016 1851482694 145.4994543678 20191016 1851472920 145.4994543678 20191016 1851472920 145.6941500473 20191016 1851482694 145.6941500473 20191016 1851482945 145.6990174393 20191016 1851482945 146.6676284448 20191016 1851492970 146.6676284448 20191016 1851492970 146.4729327653 20191016 1851502740 146.4729327653 20191016 1851502990 146.4778001573 20191016 1851502990 147.0570198039 20191016 1851522770 147.0570198039 20191016 1851523020 147.0618871959 20191016 1851523020 147.4464111629 20191016 1851533040 147.4464111629 20191016 1851533040 147.2517154834 20191016 1851542805 147.2517154834 20191016 1851543055 147.2565828754 20191016 1851543055 147.4464111629 20191016 1851552820 147.4464111629 20191016 1851553070 147.4512785549 20191016 1851553070 147.8358025219 20191016 1851563090 147.8358025219 20191016 1851563090 147.6411068424 20191016 1851573110 147.6411068424 20191016 1851573110 147.0570198039 20191016 1851563090 147.0570198039 20191016 1851563090 146.4729327653 20191016 1851573110 146.4729327653 20191016 1851573110 145.4994543678 20191016 1851563090 145.4994543678 20191016 1851563090 144.7206716498 20191016 1851553070 144.7206716498 20191016 1851553070 143.9418889317 20191016 1851543055 143.9418889317 20191016 1851543055 143.1631062137 20191016 1851533040 143.1631062137 20191016 1851533040 142.9684105342 20191016 1851523020 142.9684105342 20191016 1851523020 142.7737148547 20191016 1851513005 142.7737148547 20191016 1851513005 143.3529345012 20191016 1851512755 143.3578018932 20191016 1851482945 143.3578018932 20191016 1851482945 143.7423258602 20191016 1851482694 143.7471932522 20191016 1851472920 143.7471932522 1
Region 26
"""

if __name__ == "__main__":
    cruise = open_zarr_store()
    open_evr_file(cruise)
