import numpy as np
import xarray as xr


class CruiseManager:
    #######################################################
    def __init__(
        self,
    ):
        self.DECIMAL_PRECISION: int = 6
        self.level: str = "level_2a"

    def get_cruise(
        self,
        bucket_name: str = "noaa-wcsd-zarr-pds",
        ship_name: str = "Henry_B._Bigelow",
        cruise_name: str = "HB1906",
        sensor_name: str = "EK60",
    ):
        try:
            zarr_store = f"{cruise_name}.zarr"
            s3_zarr_store_path = f"{bucket_name}/{self.level}/{ship_name}/{cruise_name}/{sensor_name}/{zarr_store}"

            kwargs = {"consolidated": False}
            cruise = xr.open_dataset(
                f"s3://{s3_zarr_store_path}",
                engine="zarr",
                storage_options={"anon": True},
                # chunks={},
                **kwargs,
            )
            return cruise
        except Exception as e:
            print(f"Could not open cruise: {e}")

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
            cruise = self.get_cruise(
                bucket_name="noaa-wcsd-zarr-pds",
                ship_name="Henry_B._Bigelow",
                cruise_name="HB1906",
                sensor_name="EK60",
            )
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
