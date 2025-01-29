import xarray as xr
import avro
import time
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import datetime




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

# {
#     "Info": {
#         "version": "25.1.2",
#         "ship": "Henry_B._Bigelow",
#         "cruise": "HB0707",
#         "sensor": "EK60"
#     },
#     "Annotations": [
#         {
#             "area": 2351.45,
#             "bbox": [
#                 56.97,
#                 56.97,
#                 56.97,
#                 56.97
#             ],
#             "geometry": {
#                 "type": "Polygon",
#                 "coordinates": [
#                     [
#                         [100, 0],
#                         [101, 0],
#                         [101, 1],
#                         [100, 1],
#                         [100, 0]
#                     ]
#                 ]
#             }
#         }
#     ]
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
    writer.append({"name": "User2", "kitten": 123})
    writer.close()

    reader = DataFileReader(open("users.avro", "rb"), DatumReader())
    for user in reader:
        print(user)

    reader.close()

def read_water_column_schema():

    # https://avro.apache.org/docs/1.11.1/specification/
    # Parsing multiple schemas: https://stackoverflow.com/questions/40854529/nesting-avro-schemas
    schema = avro.schema.parse(open("./schema/water-column-sonar-annotation.avsc", "rb").read())

    # now = datetime.utcnow()  # or datetime.now(your_timezone)
    # foodate = now.strftime("%Y-%m-%d %H:%M:%S.%f")
    # foodate = round(time.time() * 1000) # ISO 8601 is better

    writer = DataFileWriter(open("wcsa.avro", "wb"), DatumWriter(), schema)
    writer.append(
        {
            "version": "25.1.2",
            "ship": "Henry_B._Bigelow",
            "cruise": "HB0707",
            "sensor": "EK60",
            "area": 111.11,
            # TODO: need to add a classification for the output
            "bbox": [
                56.97,
                56.97,
                56.97,
                56.97
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [datetime.datetime.now().isoformat(), 10], # e.g. ['2025-01-29T15:16:35.943485', 10]
                        [datetime.datetime.now().isoformat(), 11],
                        [datetime.datetime.now().isoformat(), 12],
                        [datetime.datetime.now().isoformat(), 13],
                        [datetime.datetime.now().isoformat(), 14]
                    ]
                ]
            }
        }
    )
    writer.append(
        {
            "version": "25.1.2",
            "ship": "Henry_B._Bigelow",
            "cruise": "HB0707",
            "sensor": "EK60",
            "area": 222.2,
            "bbox": [
                22,
                23,
                24,
                25
            ],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [datetime.datetime.now().isoformat(), 0],
                        [datetime.datetime.now().isoformat(), 1],
                        [datetime.datetime.now().isoformat(), 2],
                        [datetime.datetime.now().isoformat(), 3],
                        [datetime.datetime.now().isoformat(), 4]
                    ]
                ]
            }
        }
    )
    writer.close()

    reader = DataFileReader(open("wcsa.avro", "rb"), DatumReader())
    for annotation in reader:
        print(annotation)

    # TODO: create polygon from annotation

    # TODO: mask xarray DataArray with polygon

    # TODO: sample the data

    reader.close()



if __name__ == '__main__':
    # print(version("xarray"))
    # run_process2()
    read_water_column_schema()
