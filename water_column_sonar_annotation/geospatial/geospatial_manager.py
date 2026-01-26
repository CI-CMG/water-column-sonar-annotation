from datetime import datetime

import geopandas as gpd
import numpy as np
from dateutil import tz
from shapely import Point
from timezonefinder import TimezoneFinder

"""
Gets the distance between a point and a coastline
https://www.kaggle.com/code/notcostheta/shortest-distance-to-a-coastline
https://www.naturalearthdata.com/downloads/50m-physical-vectors/50m-coastline/

Well known text map: https://wktmap.com/
Calculate distance map: https://www.calcmaps.com/map-distance/
"""


class GeospatialManager:
    #######################################################
    def __init__(
        self,
    ):
        self.DECIMAL_PRECISION = 6
        self.crs = "EPSG:4326"  # "EPSG:3857"  # "EPSG:4326"

    def check_distance_from_coastline(
        self,  # -30.410156 51.508742)
        latitude: float = 51.508742,  # 42.682435,
        longitude: float = -30.410156,  # -68.741455,
        shapefile_path: str = None,
    ) -> float | None:
        try:
            # requires the shape file too
            geometry_one = gpd.read_file(f"{shapefile_path}/ne_50m_coastline.shp")
            geometry_one = geometry_one.set_crs(self.crs)
            geometry_two = Point([longitude, latitude])
            gdf_p = gpd.GeoDataFrame(geometry=[geometry_two], crs=self.crs)
            gdf_l = geometry_one
            gdf_p = gdf_p.to_crs(gdf_p.estimate_utm_crs())
            gdf_l = gdf_l.to_crs(gdf_p.crs)
            # TODO: index 1399 has inf values, investigate
            all_distances = [
                gdf_p.geometry.distance(gdf_l.get_geometry(0)[i])[0]
                for i in range(len(gdf_l.get_geometry(0)))
            ]
            return np.min(all_distances)
        except Exception as e:
            print(f"Could not process the distance: {e}")

    def get_local_time(
        self,
        iso_time: str = "2026-01-26T20:35:00Z",
        latitude: float = 51.508742,
        longitude: float = -30.410156,
    ) -> str:
        # https://www.geeksforgeeks.org/python/get-time-zone-of-a-given-location-using-python/
        # latitude = 51.508742
        # longitude = -30.410156
        obj = TimezoneFinder()
        calculated_timezone = obj.timezone_at(lng=longitude, lat=latitude)
        from_zone = tz.gettz("UTC")
        to_zone = tz.gettz(calculated_timezone)
        utc = datetime.fromisoformat(iso_time)
        utc = utc.replace(tzinfo=from_zone)
        local_time = utc.astimezone(to_zone)
        return local_time.isoformat()  # [:19]

    def get_hour_of_day(
        self,
        iso_time: str = "2026-01-26T20:35:00Z",
        latitude: float = 51.508742,
        longitude: float = -30.410156,
    ) -> int:
        local_time = self.get_local_time(
            iso_time=iso_time,
            latitude=latitude,
            longitude=longitude,
        )
        return int(local_time[11:13])

    def get_month_of_year(
        self,
    ):
        pass


#
# if __name__ == "__main__":
#     geospatial_manager = GeospatialManager()
#     # x = geospatial_manager.check_distance_from_coastline()
#     x = geospatial_manager.get_local_time(
#         iso_time="2026-01-26T20:35:00Z",
#         latitude=51.508742,
#         longitude=-30.410156,
#     )
#     print(x)
