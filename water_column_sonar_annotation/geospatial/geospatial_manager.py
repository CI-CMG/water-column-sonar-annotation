import geopandas as gpd
import numpy as np
from shapely import Point

"""
Gets the distance between a point and a coastline
https://www.kaggle.com/code/notcostheta/shortest-distance-to-a-coastline
https://www.naturalearthdata.com/downloads/50m-physical-vectors/50m-coastline/
"""


class GeospatialManager:
    #######################################################
    def __init__(
        self,
    ):
        self.DECIMAL_PRECISION = 6
        self.crs = "EPSG:4326"  # "EPSG:3857"  # "EPSG:4326"

    def check_distance(
        self,
        latitude: float = 42.682435,
        longitude: float = -68.741455,
    ) -> float | None:
        try:
            geometry_one = gpd.read_file(
                "ne_50m_coastline.shx"
            )  # but it requires the shape file too?
            geometry_one = geometry_one.set_crs(self.crs)
            geometry_two = Point([longitude, latitude])
            gdf_p = gpd.GeoDataFrame(geometry=[geometry_two], crs=self.crs)
            gdf_l = geometry_one
            gdf_p = gdf_p.to_crs(gdf_p.estimate_utm_crs())
            gdf_l = gdf_l.to_crs(gdf_p.crs)
            # 1399
            all_distances = [
                gdf_p.geometry.distance(gdf_l.get_geometry(0)[i])[0]
                for i in range(len(gdf_l.get_geometry(0)))
                # if gdf_p.geometry.distance(gdf_l.get_geometry(0)[i])[0] != np.inf
            ]
            return np.min(all_distances)
        except Exception as e:
            print(f"Could not process the distance: {e}")


if __name__ == "__main__":
    geospatial_manager = GeospatialManager()
    distance = geospatial_manager.check_distance()
    print(distance)
