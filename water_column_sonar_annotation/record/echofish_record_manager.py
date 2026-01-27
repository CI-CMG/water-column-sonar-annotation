from typing import Optional


"""
Format for export to parquet and bulk ingest into neo4j:
"""
# TODO:
#  [1] write the records to a pandas dataframe
#  [2] write df to parquet and tag as github resource


class EchofishRecordManager:
    def __init__(
        self,
        polygon: str,
        ### geospatial ###
        start_time: str,
        end_time: str,
        min_depth: float,
        max_depth: float,
        month: int,
        altitude: float,
        latitude: float,
        longitude: float,
        local_time: str,
        distance_from_coastline: float,
        ### astronomical ###
        solar_altitude: float,
        is_daytime: bool,
        ### provenance ###
        provenance: Optional[str] = None,
        ship: str = "Henry_B._Bigelow",
        cruise: str = "HB1906",
        sensor: str = "EK60",
    ):
        print("__init__ called")

    def __enter__(self):
        print("__enter__ called")
        return self

    def __exit__(self, *a):
        print("__exit__ called")

    def echofish_record(
        self,
    ):
        try:
            pass
        except Exception as echofish_record_exception:
            print(f"Problem with echofish_record: {echofish_record_exception}")
