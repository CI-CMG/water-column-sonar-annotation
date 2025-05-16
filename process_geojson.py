import fiona
import geopandas as gpd

bucket_name = "noaa-wcsd-zarr-pds"
ship_name = "Henry_B._Bigelow"
cruise_name = "HB1906"
sensor_name = "EK60"


def open_geojson():
    data = gpd.read_file("point_dataset.geojson")
    print(data)
    annotation_labels = set(data["label"])
    # TODO: iterate, getting features out
    for annotation_label in annotation_labels:
        print(f"Processing: {annotation_label}")
        data_select = data[data["label"] == annotation_label]
        # create unique ids
        data_select["id"] = range(data_select.shape[0])
        # write out using proper pm-tile format
        gps_gdf = gpd.GeoDataFrame(
            data=data_select[["id", "time", "ship", "cruise", "sensor", "label"]],
            geometry=data_select["geometry"],
            crs="EPSG:4326",
        )
        # TODO: remove null values

        # {'DXF': 'rw', 'CSV': 'raw', 'OpenFileGDB': 'raw', 'ESRIJSON': 'r', 'ESRI Shapefile': 'raw', 'FlatGeobuf': 'raw', 'GeoJSON': 'raw', 'GeoJSONSeq': 'raw', 'GPKG': 'raw', 'GML': 'rw', 'OGR_GMT': 'rw', 'GPX': 'rw', 'MapInfo File': 'raw', 'DGN': 'raw', 'S57': 'r', 'SQLite': 'raw', 'TopoJSON': 'r'}
        if "GeoJSON" not in fiona.supported_drivers.keys():
            raise RuntimeError("Missing GeoJSON driver")
        gps_gdf.set_index("id", inplace=True)
        # write to file
        if gps_gdf.isnull().values.any():
            print("problem")
            gps_gdf.dropna(inplace=True)
        gps_gdf.to_file(
            filename=f"point_dataset_{annotation_label}.geojson",
            driver="GeoJSON",
            engine="fiona",  # or "pyogrio"
            layer_options={"ID_GENERATE": "YES"},
            crs="EPSG:4326",
            id_generate=True,  # required for the feature click selection
        )

    print(
        # TODO: above read into multiple geojson datasets, get ids of each, then write multiple files for multiple layers
        'Now run this: "tippecanoe --no-feature-limit -zg -o point_dataset.pmtiles -l biome point_dataset.geojson --force"'
    )


"""
#-l AH_School point_dataset_AH_School.geojson -l Unclassified point_dataset_Unclassified_regions.geojson -l Atlantic_Herring point_dataset_atlantic_herring.geojson -l Fish_School point_dataset_fish_school.geojson -l Krill_Schools point_dataset_krill_schools.geojson -l Possible_Herring point_dataset_possible_herring.geojson
tippecanoe --no-feature-limit -zg -o point_dataset.pmtiles -l AH_School point_dataset_AH_School.geojson --force
tippecanoe --no-feature-limit -zg -o point_dataset.pmtiles -L AH_School:point_dataset_AH_School.geojson -L Unclassified_regions:point_dataset_Unclassified_regions.geojson -L Atlantic_Herring:point_dataset_atlantic_herring.geojson -L Fish_School:point_dataset_fish_school.geojson -L Krill_Schools:point_dataset_krill_schools.geojson -L Possible_Herring:point_dataset_possible_herring.geojson --force

tippecanoe --no-feature-limit -zg -o point_dataset.pmtiles point_dataset_AH_School.geojson point_dataset_Unclassified_regions.geojson point_dataset_atlantic_herring.geojson point_dataset_fish_school.geojson point_dataset_krill_schools.geojson point_dataset_possible_herring.geojson --force
"""

if __name__ == "__main__":
    open_geojson()
