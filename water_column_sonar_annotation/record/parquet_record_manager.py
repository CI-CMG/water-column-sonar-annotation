from json import dumps

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# from pyspark.sql.functions import *

"""
Format for export to parquet and bulk ingest into neo4j:
"""
# TODO:
#  [1] write the records to a pandas dataframe
#  [2] write df to parquet and tag as github resource


class ParquetRecordManager:
    def __init__(
        self,
        # classification,
        # point_count,
        # geometry,
        # time_start,
        # time_end,
        # depth_min,
        # depth_max,
        # month,
        # # altitude,
        # # latitude: float,
        # # longitude: float,
        # # local_time,
        # # distance_from_coastline,
        # # solar_altitude,
        # # is_daytime,
        # filename,
        # region_id,
        # geometry_hash,  # sha256 hash
        ship: str = "Henry_B._Bigelow",
        cruise: str = "HB1906",
        instrument: str = "EK60",
    ):
        print("__init__ called")
        # self.classification: str = classification
        # self.point_count: int = point_count
        # # self.geometry: str = geometry
        # ### geospatial ###
        # self.time_start: str = time_start
        # self.time_end: str = time_end
        # self.depth_min: float = depth_min
        # self.depth_max: float = depth_max
        # self.month: int = month
        # self.altitude: float = altitude
        # self.latitude: float = latitude
        # self.longitude: float = longitude
        # self.local_time: str = local_time
        # self.distance_from_coastline: float = distance_from_coastline
        # ### astronomical ###
        # self.solar_altitude: float = solar_altitude
        # self.is_daytime: bool = is_daytime
        ### provenance ###
        # self.filename: str = filename
        # self.region_id: str = region_id
        # self.geometry_hash: str = geometry_hash
        self.ship: str = ship
        self.cruise: str = cruise
        self.instrument: str = instrument

    # def __enter__(self):
    #     print("__enter__ called")
    #     return self

    # def __exit__(self, *a):
    #     print("__exit__ called")

    def save_test_data(self):
        # test_data = "geometry_hash,classification,point_count,time_start,time_end,depth_min,depth_max,month,altitude,latitude,longitude,local_time,distance_from_coastline,solar_altitude,filename,region_id,ship,cruise,instrument,phase_of_day
        # [e78ee8839c5bd4931b0a790dabe334d5f9200e80b9e4057e5e1a62f60a14e5cf,Unclassified regions,15,2019-09-25T14:02:06.601000,2019-09-25T14:02:57.165800,8.85,14.37,9,-2.88,41.5303955078125,-71.318603515625,2019-09-25T10:02:06.601000-04:00,250.0,35.02,d20190925_t135327-t233118_Zsc-DWBA-Schools_All-RegionDefs.evr,2,Henry_B._Bigelow,HB1906,EK60,2]
        # [46909f534985668542b6437224f0a533a8960619d93247fca0477995e559d9c0,possible_herring,8,2019-09-25T17:49:38.647000,2019-09-25T17:49:57.674000,18.75,29.31,9,3.4,41.38581466674805,-71.3131332397461,2019-09-25T13:49:38.647000-04:00,8139.0,44.57,d20190925_t135327-t233118_Zsc-DWBA-Schools_All-RegionDefs.evr,3,Henry_B._Bigelow,HB1906,EK60,2]
        # [20d22a2da4b120ba925abe0eb39aabfa29dc9b6990888e268d0ea8a3c76511bc,fish_school,30,2019-09-25T17:49:45.160500,2019-09-25T17:49:50.164500,20.28,26.3,9,6.83,41.38582992553711,-71.31282806396484,2019-09-25T13:49:45.160500-04:00,8146.0,44.57,d20190925_t135327-t233118_Zsc-DWBA-Schools_All-RegionDefs.evr,8,Henry_B._Bigelow,HB1906,EK60,2]
        times_start = pd.to_datetime(
            [  # dfp["time_start"] >= np.datetime64("2019-09-25T17:49:38.647000")
                "2019-09-25T14:02:06.601000",
                "2019-09-25T17:49:38.647000",
                "2019-09-25T17:49:45.160500",
            ]
        )
        times_end = pd.to_datetime(
            [
                "2019-09-25T14:02:57.165800",
                "2019-09-25T17:49:57.674000",
                "2019-09-25T17:49:50.164500",
            ]
        )
        geometry_hashes = [
            "e78ee8839c5bd4931b0a790dabe334d5f9200e80b9e4057e5e1a62f60a14e5cf",
            "46909f534985668542b6437224f0a533a8960619d93247fca0477995e559d9c0",
            "20d22a2da4b120ba925abe0eb39aabfa29dc9b6990888e268d0ea8a3c76511bc",
        ]
        df = pd.DataFrame(
            data={
                "time_start": times_start,
                "time_end": times_end,
                "geometry_hash": geometry_hashes,
            },
            # index=["geometry_hashes"],
        )
        # df.set_index("geometry_hash", drop=True, inplace=True)
        df.to_parquet(
            path="test.parquet",
            engine="pyarrow",
            compression="snappy",
            index=True,
            # partition_cols=df.columns,
        )
        print("done")
        # df = pd.read_csv("Henry_B._Bigelow_HB1906_annotations.parquet")
        #
        ### now test reading ###
        #
        dfp = pq.read_table(source="test.parquet")
        print(dfp.shape)
        dfp_select = dfp.filter(
            dfp["time_start"] >= np.datetime64("2019-09-25T17:49:38.647000")
        )
        print(dfp_select.shape)
        # df = pq.read_table(source="Henry_B._Bigelow_HB1906_annotations.parquet").to_pandas()

    def to_dict(
        self,
    ):
        try:
            return self.__dict__
        except Exception as parquet_record_exception:
            print(f"Problem with parquet record: {parquet_record_exception}")

    def to_json(
        self,
    ):
        try:
            return dumps(self.__dict__)
        except Exception as parquet_record_exception:
            print(f"Problem with parquet record: {parquet_record_exception}")


# if __name__ == "__main__":
#     try:
#         parquet_record_manager = ParquetRecordManager()
#         parquet_record_manager.save_test_data()
#     except Exception as e:
#         print(e)


"""
with pandas:
    pyarrow.Table
    time_start: timestamp[us]
    time_end: timestamp[us]
    geometry_hash: large_string
with numpy datetime64
"""
