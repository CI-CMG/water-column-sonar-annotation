from importlib.metadata import version
import xarray as xr
import avro
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


"""
Working through the tutorial

TODO:
    explore: fastavro as alternative
    https://fastavro.readthedocs.io/en/latest/

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

"""
BT
20081013 0945237040 0.097133 20081013 0945237040 499.942520 20081013 1005300480 499.942520 20081013 1005300480 0.097133 1 
251_175_000
"""
def run_process2():
    # Following this guide: https://avro.apache.org/docs/1.11.1/getting-started-python/
    print('Processing: water-column-sonar-annotation.avsc')

    # https://avro.apache.org/docs/1.11.1/specification/
    # Parsing multiple schemas: https://stackoverflow.com/questions/40854529/nesting-avro-schemas
    schema = avro.schema.parse(open("./schema/water-column-sonar-annotation.avsc", "rb").read())

    writer = DataFileWriter(open("water-column-sonar-annotations.avro", "wb"), DatumWriter(), schema)
    writer.append(
        {
            "name": "Alyssa",
            "favorite_number": 256
        }
    )
    writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
    writer.append({"name": "rudu", "kitten": 123})
    writer.close()

    reader = DataFileReader(open("users.avro", "rb"), DatumReader())
    for user in reader:
        print(user)

    reader.close()



if __name__ == '__main__':
    print(version("xarray"))
    run_process2()
