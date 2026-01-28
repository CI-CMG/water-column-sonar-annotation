import numpy as np
import xarray as xr


class CruiseManager:
    #######################################################
    def __init__(
        self,
        bucket_name: str = "noaa-wcsd-zarr-pds",
        ship_name: str = "Henry_B._Bigelow",
        cruise_name: str = "HB1906",
        sensor_name: str = "EK60",
    ):
        self.level: str = "level_2a"
        self.bucket_name: str = bucket_name
        self.ship_name: str = ship_name
        self.cruise_name: str = cruise_name
        self.sensor_name: str = sensor_name
        try:
            zarr_store = f"{self.cruise_name}.zarr"
            s3_zarr_store_path = f"{self.bucket_name}/{self.level}/{self.ship_name}/{self.cruise_name}/{self.sensor_name}/{zarr_store}"

            kwargs = {"consolidated": False}
            cruise = xr.open_dataset(
                f"s3://{s3_zarr_store_path}",
                engine="zarr",
                storage_options={"anon": True},
                # chunks={},
                **kwargs,
            )
            self.cruise = cruise
        except Exception as e:
            print(f"Could not open cruise: {e}")

    def get_cruise(
        self,
    ):
        try:
            # zarr_store = f"{self.cruise_name}.zarr"
            # s3_zarr_store_path = f"{self.bucket_name}/{self.level}/{self.ship_name}/{self.cruise_name}/{self.sensor_name}/{zarr_store}"
            #
            # kwargs = {"consolidated": False}
            # cruise = xr.open_dataset(
            #     f"s3://{s3_zarr_store_path}",
            #     engine="zarr",
            #     storage_options={"anon": True},
            #     # chunks={},
            #     **kwargs,
            # )
            return self.cruise
        except Exception as e:
            print(f"Could not open cruise: {e}")

    def get_coordinates(
        self,
        start_time,  # ="2019-10-16T16:20:00",  # In UTC
        end_time,
    ):
        """gets the gps coordinates using the time & cruise"""
        try:
            cruise_select = self.cruise.sel(time=slice(start_time, end_time))
            return cruise_select.latitude.values[0], cruise_select.longitude.values[0]
        except Exception as e:
            print(f"Could not find depth: {e}")

    def get_depth(
        self,
        start_time="2019-10-16T16:20:00",
        end_time="2019-10-16T16:50:00",
    ):
        """
        Returns the bottom depth in meters for a given ISO timestamp
        Value returned is the minimum depth over that interval.
        """
        try:
            cruise = self.cruise  # get_cruise()
            time_slice = slice(start_time, end_time)
            bottom_depths = cruise.sel(time=time_slice).bottom.values
            return np.nanmin(bottom_depths)
        except Exception as e:
            print(f"Could not find depth: {e}")

    def get_altitude(
        self,
        start_time="2019-10-16T16:20:00",
        end_time="2019-10-16T16:50:00",
    ):
        pass


# if __name__ == "__main__":
#     astronomical_manager = AstronomicalManager()
#     azimuth = astronomical_manager.get_solar_azimuth()
#     print(azimuth)
