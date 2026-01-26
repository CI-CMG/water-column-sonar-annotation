import xarray as xr


class CruiseManager:
    #######################################################
    def __init__(
        self,
    ):
        self.DECIMAL_PRECISION = 6

    def get_cruise(
        self,
        bucket_name: str = "noaa-wcsd-zarr-pds",
        level: str = "level_2a",
        ship_name: str = "Henry_B._Bigelow",
        cruise_name: str = "HB1906",
        sensor_name: str = "EK60",
    ):
        try:
            zarr_store = f"{cruise_name}.zarr"
            s3_zarr_store_path = f"{bucket_name}/{level}/{ship_name}/{cruise_name}/{sensor_name}/{zarr_store}"

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


# if __name__ == "__main__":
#     astronomical_manager = AstronomicalManager()
#     azimuth = astronomical_manager.get_solar_azimuth()
#     print(azimuth)
