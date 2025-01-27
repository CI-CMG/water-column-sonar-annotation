from importlib.metadata import version
import xarray as xr
import avro
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


"""
STEP 4 -- Verify cruise HB0707.
"""

# https://colab.research.google.com/drive/1-I56QOIftj9sewlbyzTzncdRt54Fh51d?usp=sharing#scrollTo=mM49CCneMgBx
def run_process():
    bucket_name = 'noaa-wcsd-zarr-pds'
    ship_name = "Henry_B._Bigelow"
    cruise_name = "HB0707"
    sensor_name = "EK60"
    zarr_store = f'{cruise_name}.zarr'
    s3_zarr_store_path = f"{bucket_name}/level_2/{ship_name}/{cruise_name}/{sensor_name}/{zarr_store}"
    #store = s3fs.S3Map(root=s3_zarr_store_path, s3=s3_file_system, check=False)
    cruise = xr.open_dataset(f"s3://{s3_zarr_store_path}", storage_options={'anon': True})
    print(cruise)

# {"namespace": "example.avro",
#  "type": "record",
#  "name": "User",
#  "fields": [
#      {"name": "name", "type": "string"},
#      {"name": "favorite_number",  "type": ["int", "null"]},
#      {"name": "favorite_color", "type": ["string", "null"]}
#  ]
# }
def run_process2():
    # Following this guide: https://avro.apache.org/docs/1.11.1/getting-started-python/
    print('Processing: user.avsc')

    schema = avro.schema.parse(open("./schema/user.avsc", "rb").read())

    writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)
    writer.append({"name": "Alyssa", "favorite_number": 256})
    writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
    writer.append({"name": "rudu"})
    writer.close()

    reader = DataFileReader(open("users.avro", "rb"), DatumReader())
    for user in reader:
        print(user)

    reader.close()



if __name__ == '__main__':
    print(version("xarray"))
    run_process2()
