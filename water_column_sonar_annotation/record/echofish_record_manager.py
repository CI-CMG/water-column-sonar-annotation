from json import dumps

"""
Format for export to parquet and bulk ingest into neo4j:
"""
# TODO:
#  [1] write the records to a pandas dataframe
#  [2] write df to parquet and tag as github resource


class EchofishRecordManager:
    def __init__(
        self,
        geometry,
        start_time,
        end_time,
        min_depth,
        max_depth,
        month,
        altitude,
        latitude: float,
        longitude: float,
        local_time,
        distance_from_coastline,
        solar_altitude,
        is_daytime,
        provenance,
        ship: str = "Henry_B._Bigelow",
        cruise: str = "HB1906",
        sensor: str = "EK60",
    ):
        print("__init__ called")
        self.geometry: str = geometry
        ### geospatial ###
        self.start_time: str = start_time
        self.end_time: str = end_time
        self.min_depth: float = min_depth
        self.max_depth: float = max_depth
        self.month: int = month
        self.altitude: float = altitude
        self.latitude: float = latitude
        self.longitude: float = longitude
        self.local_time: str = local_time
        self.distance_from_coastline: float = distance_from_coastline
        ### astronomical ###
        self.solar_altitude: float = solar_altitude
        self.is_daytime: bool = is_daytime
        ### provenance ###
        self.provenance: str = provenance
        self.ship: str = ship
        self.cruise: str = cruise
        self.sensor: str = sensor

    def __enter__(self):
        print("__enter__ called")
        return self

    def __exit__(self, *a):
        print("__exit__ called")

    def echofish_record_to_json(
        self,
    ):
        try:
            dumps(self.__dict__)
        except Exception as echofish_record_exception:
            print(f"Problem with echofish_record: {echofish_record_exception}")
