from os import listdir
from os.path import isfile, join
from pathlib import Path
from typing import Optional

import pandas as pd
import pvlib
import xarray as xr

"""
https://support.echoview.com/WebHelp/Reference/File_Formats/Export_File_Formats/2D_Region_definition_file_format.htm

"""


class EchoviewRecord:
    def __init__(
        self,
        # endpoint_url: Optional[str] = None,
    ):
        print("__init__ called")
        self.region_creation_type = {  # Data formats — The region creation type is one of the following
            "-1": "No type",
            "0": "Created from a selection made using the horizontal band tool horizontal selection tool",
            "1": "Created from a selection made using the parallelogram tool parallelogram tool",
            "2": "Created from a selection made using the polygon tool polygon selection tool",
            "3": "Created from a selection made using the rectangle tool rectangle tool",
            "4": "Created from a selection made using the vertical band tool vertical selection tool",
            "5": "Created as a bottom-relative region or line-relative region",
            "6": "Created or assigned as Marker region.",
            "7": "Created using the Detect Schools command",
            "8": "Invalid or unknown region type",
            "9": "Created as a fish track region",
        }
        self.region_type = {
            "0": "bad (no data)",
            "1": "analysis",
            "2": "marker",
            "3": "fishtracks",
            "4": "bad (empty water)",
        }
        #
        self.region_structure_version = None  # "13" (will be incremented if the region structure changes in future versions)
        self.point_count = None  # Number of points in the region
        self.region_id = None  # # Unique number for each region. Specify sequential numbers starting at 1 if creating a new file
        self.selected = None  # "0" (always)
        self.region_creation_type = None  # See "Data formats" definition
        self.dummy = None  # Should always be "-1"
        self.bounding_rectangle_calculated = (
            None  # "1" if the next four fields are valid; "0" otherwise
        )
        self.left_x_value_of_bounding_rectangle: Optional[str] = (
            None  # Date and time of left boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
        )
        self.top_y_value_of_bounding_rectangle: Optional[str] = (
            None  # Upper depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
        )
        self.right_x_value_of_bounding_rectangle: Optional[str] = (
            None  # Date and time of right boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
        )
        self.bottom_y_value_of_bounding_rectangle: Optional[str] = (
            None  # Lower depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
        )
        self.number_of_lines_of_notes = (
            None  # The number of lines of region notes to follow.
        )
        self.region_notes: Optional[str] = (
            None  # Notes associated with the region. Maximum length is 2048 characters. Embedded CR characters are encoded as hexadecimal FF. Embedded LF characters are encoded as hexadecimal FE.
        )
        self.number_of_lines_of_detection_settings = (
            None  # The number of lines of detection settings to follow.
        )
        self.region_detection_settings: Optional[str] = (
            None  # The detection settings as defined in the Fish Track Detection Properties dialog box or Detect Schools dialog box.
        )
        self.region_classification = None  # Region classification (string). Default value is "Unclassified regions"
        self.points = None  # Data for first point – See Data formats below. These data are used to bound the region when importing into Echoview
        self.region_type = None  # "0" = bad (no data); "1" = analysis; "2" = marker, "3" = fishtracks; "4" = bad (empty water);
        self.region_name = None  # String

    def __enter__(self):
        print("__enter__ called")
        return self

    def __exit__(self, *a):
        print("__exit__ called")

    @staticmethod
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            # yield lst[i:i + n]
            yield " ".join(lst[i : i + n])

    def ingest_region(self, region):
        """
        TODO: ingest a record, get the bbox, polygon, and label
        #
        Convert data to dataframe and save to parquet file compressed, 1-file
        # TODO: tag & release
        #
        Need way to convert to geopandas?!
        :param region:
        :return:
        """
        # evr_region_structure_version = bbox_split[0] # "13" (will be incremented if the region structure changes in future versions)
        # evr_point_count = bbox_split[1] # Number of points in the region
        # evr_region_id = # Unique number for each region. Specify sequential numbers starting at 1 if creating a new file
        # evr_selected = # "0" (always)
        # evr_region_creation_type = # See "Data formats" definition
        # evr_dummy = # Should always be "-1"
        # evr_bounding_rectangle_calculated = # "1" if the next four fields are valid; "0" otherwise
        # evr_left_x_value_of_bounding_rectangle = # Date and time of left boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
        # evr_top_y_value_of_bounding_rectangle = # Upper depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
        # evr_right_x_value_of_bounding_rectangle = # Date and time of right boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
        # evr_bottom_y_value_of_bounding_rectangle = # Lower depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
        # evr_number_of_lines_of_notes = # The number of lines of region notes to follow.
        # evr_region_notes = # Notes associated with the region. Maximum length is 2048 characters. Embedded CR characters are encoded as hexadecimal FF. Embedded LF characters are encoded as hexadecimal FE.
        # evr_number_of_lines_of_detection_settings = # The number of lines of detection settings to follow.
        # evr_region_detection_settings = # The detection settings as defined in the Fish Track Detection Properties dialog box or Detect Schools dialog box.
        # evr_region_classification = # Region classification (string). Default value is "Unclassified regions"
        # evr_points = # Data for first point – See Data formats below. These data are used to bound the region when importing into Echoview
        # evr_region_type = # "0" = bad (no data); "1" = analysis; "2" = marker, "3" = fishtracks; "4" = bad (empty water);
        # evr_region_name = # String
        pass

    def process_point_data(self, point_data):
        foo = list(self.chunks(point_data, 3))
        print(foo)
        return foo


def open_evr_file():  # model_cruise):
    """
    Open evr file and create records for each entry
    # :param model_cruise:
    :return:
    """
    # print(model_cruise)
    mypath = "../data/HB201906/"
    all_evr_files = [
        f
        for f in listdir(mypath)
        if isfile(join(mypath, f)) and Path(f).suffix == ".evr"
    ]
    all_evr_files.sort()
    for evr_file in all_evr_files:
        print(evr_file)
        with open(mypath + evr_file, "r") as file:
            lines = file.read()
        records = lines.split("\n\n")
        #
        #
        #
        records = [i for i in records if i.startswith("13 ")]  # filter
        for record in records:
            print("_+_+_+_+ start new record _+_+_+")
            record_lines = record.split("\n")
            ####################################
            ############# get bbox #############
            bbox_split = record_lines[0].split()  # [x for x in record.split() if x]
            ###
            # https://support.echoview.com/WebHelp/Reference/File_Formats/Export_File_Formats/2D_Region_definition_file_format.htm
            # evr_region_structure_version = bbox_split[0] # "13" (will be incremented if the region structure changes in future versions)
            # evr_point_count = bbox_split[1] # Number of points in the region
            # evr_region_id = # Unique number for each region. Specify sequential numbers starting at 1 if creating a new file
            # evr_selected = # "0" (always)
            # evr_region_creation_type = # See "Data formats" definition
            # evr_dummy = # Should always be "-1"
            # evr_bounding_rectangle_calculated = # "1" if the next four fields are valid; "0" otherwise
            # evr_left_x_value_of_bounding_rectangle = # Date and time of left boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
            # evr_top_y_value_of_bounding_rectangle = # Upper depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
            # evr_right_x_value_of_bounding_rectangle = # Date and time of right boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
            # evr_bottom_y_value_of_bounding_rectangle = # Lower depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
            # evr_number_of_lines_of_notes = # The number of lines of region notes to follow.
            # evr_region_notes = # Notes associated with the region. Maximum length is 2048 characters. Embedded CR characters are encoded as hexadecimal FF. Embedded LF characters are encoded as hexadecimal FE.
            # evr_number_of_lines_of_detection_settings = # The number of lines of detection settings to follow.
            # evr_region_detection_settings = # The detection settings as defined in the Fish Track Detection Properties dialog box or Detect Schools dialog box.
            # evr_region_classification = # Region classification (string). Default value is "Unclassified regions"
            # evr_points = # Data for first point – See Data formats below. These data are used to bound the region when importing into Echoview
            # evr_region_type = # "0" = bad (no data); "1" = analysis; "2" = marker, "3" = fishtracks; "4" = bad (empty water);
            # evr_region_name = # String
            ###
            #
            ###
            time_start = bbox_split[7:9]  # bbox start time
            time_end = bbox_split[10:12]  # bbox end time
            # TODO: assert length
            # 13 12 1 0 2 -1 1 20190925 2053458953  9.2818 20190925 2054119318  11.5333
            converted_time_start = pd.to_datetime(
                f"{time_start[0]}T{time_start[1]}", format="%Y%m%dT%H%M%S%f"
            )
            converted_time_end = pd.to_datetime(
                f"{time_end[0]}T{time_end[1]}", format="%Y%m%dT%H%M%S%f"
            )
            upper_left = [converted_time_start, float(bbox_split[9])]  # [x_min, y_min]
            bottom_right = [converted_time_end, float(bbox_split[12])]  # [x_max, y_max]
            # bounding_box = record_split[0]  # TODO: get box
            # polygon_label = record_split[-3]
            print(f"bounding box: {upper_left}, {bottom_right}")
            ##########################################
            ############# get data label #############
            data_label = record_lines[-3]
            print(data_label)
            #######################################
            ############# get polygon #############
            polygon_data = record_lines[-2]
            print(polygon_data)
            #
            # [1] break up polygon
            if True:
                evr = EchoviewRecord()
                polygon_region = evr.process_point_data(polygon_data.split(" "))
                print(polygon_region)
            #
            # [3] convert each point
            #
            # [4] piece it together -> wind clockwise
            # polygon_vertices = record_split[-2]
            # all_vertices = list(
            #     zip(*[iter(polygon_vertices.split(" "))] * 3)
            # )  # date time depth
            # TODO: iterate vertices and format
            # for vertice in all_vertices:
            #    create time & depth annotation
            # combined_polygon = []
            # closest_latitude = opened_cruise.latitude.sel(
            #     time=converted_time, method="nearest"
            # )
            # also need ping_time_index?

            #############  #############
            #############  #############
            # print(
            #     f"time: {converted_time}, label: {polygon_label}"
            # )
            print("done reading")
            print("\n")

    # I don't have the lat/lon information to draw here... need to query the zarr store...

    print("done")


labels = [
    "possible_herring",
    "fish_school",
    "Unclassified regions",
    "krill_schools",
    "AH_School",
]
labels_with_extra_data = ["fish_school", "krill_schools", "AH_School"]
record_header = """EVRG 7 11.0.244.39215
11"""

possible_herring_example = """13 4 7 0 3 -1 1 20190925 2247242130  24.1290795746 20190925 2247362460  35.1668500183
0
0
possible_herring
20190925 2247242130 24.1290795746 20190925 2247242130 35.1668500183 20190925 2247362460 35.1668500183 20190925 2247362460 24.1290795746 1
100"""

fish_school_example = """13 30 8 0 7 -1 1 20190925 1749451605  20.2800268439 20190925 1749501645  26.3028953087
0
10
School detected with:
Minimum data threshold:  -66.00
Maximum data threshold: (none)
Distance mode: GPS distance
Minimum total school height (meters):   4.00
Minimum candidate length (meters):   1.00
Minimum candidate height (meters):   2.00
Maximum vertical linking distance (meters):   2.00
Maximum horizontal linking distance (meters):  20.00
Minimum total school length (meters):   4.00
fish_school
20190925 1749451605 22.0286015595 20190925 1749451605 25.1371788317 20190925 1749461355 25.1371788317 20190925 1749461605 25.1420359837 20190925 1749461605 25.7200370702 20190925 1749471370 25.7200370702 20190925 1749471620 25.7248942222 20190925 1749471620 26.1086092292 20190925 1749481385 26.1086092292 20190925 1749481635 26.1134663812 20190925 1749481635 26.3028953087 20190925 1749491640 26.3028953087 20190925 1749491640 25.1371788317 20190925 1749501645 25.1371788317 20190925 1749501645 24.5543205932 20190925 1749491640 24.5543205932 20190925 1749491640 24.3600345136 20190925 1749501645 24.3600345136 20190925 1749501645 21.6400294005 20190925 1749491640 21.6400294005 20190925 1749491640 21.0571711620 20190925 1749481635 21.0571711620 20190925 1749481635 20.2800268439 20190925 1749471620 20.2800268439 20190925 1749471620 20.8580279305 20190925 1749471370 20.8628850825 20190925 1749461605 20.8628850825 20190925 1749461605 21.8294583280 20190925 1749461355 21.8343154800 20190925 1749451605 21.8343154800 1
Region 8"""

unclassified_regions_example = """13 12 1 0 2 -1 1 20190925 2053458953  9.2818 20190925 2054119318  11.5333
0
0
Unclassified regions
20190925 2053458953 9.6034489515 20190925 2053521545 11.1197829964 20190925 2054046730 11.5333286451 20190925 2054064248 11.5333286451 20190925 2054079263 11.4414296120 20190925 2054116810 10.8440858974 20190925 2054119318 10.4764897652 20190925 2054116810 9.6953479845 20190925 2054111800 9.4196508854 20190925 2054091775 9.2818023359 20190925 2054076760 9.2818023359 20190925 2053483995 9.5574994350 0
100"""

krill_schools_example = """13 23 49 0 7 -1 1 20191020 0536541560  13.0000000000 20191020 0536591655  17.0000000000
0
10
School detected with:
Minimum data threshold:  -80.00
Maximum data threshold: (none)
Distance mode: GPS distance
Minimum total school height (meters):   4.00
Minimum candidate length (meters):   1.00
Minimum candidate height (meters):   2.00
Maximum vertical linking distance (meters):   2.00
Maximum horizontal linking distance (meters):  20.00
Minimum total school length (meters):   4.00
krill_schools
20191020 0536551605 15.0250000000 20191020 0536551605 17.0000000000 20191020 0536561605 17.0000000000 20191020 0536561605 16.0000000000 20191020 0536571635 16.0000000000 20191020 0536571635 15.0000000000 20191020 0536581405 15.0000000000 20191020 0536581655 15.0250000000 20191020 0536581655 16.0000000000 20191020 0536591655 16.0000000000 20191020 0536591655 15.0000000000 20191020 0536581655 15.0000000000 20191020 0536581655 14.0000000000 20191020 0536571635 14.0000000000 20191020 0536571635 13.0000000000 20191020 0536561605 13.0000000000 20191020 0536561605 14.9750000000 20191020 0536561355 15.0000000000 20191020 0536551605 15.0000000000 20191020 0536551605 14.0000000000 20191020 0536541560 14.0000000000 20191020 0536541560 15.0000000000 20191020 0536551355 15.0000000000 1
Region 49"""

ah_school_example = """13 5 23 0 7 -1 1 20191106 1314583780  25.3008882713 20191106 1314593790  30.2941528987
0
10
School detected with:
Minimum data threshold:  -66.00
Maximum data threshold: (none)
Distance mode: GPS distance
Minimum total school height (meters):   4.00
Minimum candidate length (meters):   1.00
Minimum candidate height (meters):   2.00
Maximum vertical linking distance (meters):   2.00
Maximum horizontal linking distance (meters):  20.00
Minimum total school length (meters):   4.00
AH_School
20191106 1314583780 25.4929369108 20191106 1314583780 30.2941528987 20191106 1314593790 30.2941528987 20191106 1314593790 25.3008882713 20191106 1314583780 25.3008882713 1
Region 23"""

# 20191106 1314583780 25.4929369108 # top-left
# 20191106 1314583780 30.2941528987 # bottom-left
# 20191106 1314593790 30.2941528987 # bottom-right
# 20191106 1314593790 25.3008882713 # top-right
# 20191106 1314583780 25.3008882713 1 # top-left'ish, ends with '1' ...goes counter-clockwise

ah_school_example2 = """13 16 28 0 7 -1 1 20191106 1317305715  31.8305420148 20191106 1317335745  35.8635634446
0
10
School detected with:
Minimum data threshold:  -66.00
Maximum data threshold: (none)
Distance mode: GPS distance
Minimum total school height (meters):   4.00
Minimum candidate length (meters):   1.00
Minimum candidate height (meters):   2.00
Maximum vertical linking distance (meters):   2.00
Maximum horizontal linking distance (meters):  20.00
Minimum total school length (meters):   4.00
AH_School
20191106 1317315725 34.3319755445 20191106 1317315725 35.8635634446 20191106 1317325735 35.8635634446 20191106 1317325735 35.4794661656 20191106 1317335745 35.4794661656 20191106 1317335745 34.5192229680 20191106 1317325735 34.5192229680 20191106 1317325735 34.3271743285 20191106 1317335745 34.3271743285 20191106 1317335745 31.8305420148 20191106 1317315725 31.8305420148 20191106 1317315725 34.1303244730 20191106 1317315475 34.1351256890 20191106 1317305715 34.1351256890 20191106 1317305715 34.3271743285 20191106 1317315475 34.3271743285 1
Region 28"""


def open_zarr_store(
    bucket_name="noaa-wcsd-zarr-pds",
    level="level_2a",
    ship_name="Henry_B._Bigelow",
    cruise_name="HB1906",
    sensor_name="EK60",
):
    try:
        zarr_store = f"{cruise_name}.zarr"
        store_path = f"s3://{bucket_name}/{level}/{ship_name}/{cruise_name}/{sensor_name}/{zarr_store}"
        kwargs = {"consolidated": False}
        return xr.open_dataset(
            filename_or_obj=store_path,
            engine="zarr",
            storage_options={"anon": True},
            **kwargs,
        )
    except Exception as e:
        print(f"could not process cruise: {e}")


class ShapeManager:
    def __init__(
        self,
    ):
        self.DEPTH_PRECISION = 4

    def point(
        self,
        date_string,
        time_string,
        depth_string,
    ):  # -> returntype # TODO:
        pass

    def polygon(
        self,
        date_string,
        time_string,
        depth_string,
    ):  # -> type # TODO:
        pass

    def bbox(
        self,
        date_string,
        time_string,
        depth_string,
    ):  # -> returntype # TODO:
        pass


def get_solar_azimuth(
    iso_time: str = "2026-01-25T14:08:06Z",
    latitude: float = 39.9674884,
    longitude: float = -105.2532602,
):
    foo123 = pvlib.solarposition.get_solarposition(
        time=pd.DatetimeIndex([iso_time]),
        latitude=latitude,
        longitude=longitude,
        altitude=0.0,
    ).azimuth.iloc[0]
    print(foo123)
    foo456 = pvlib.solarposition.sun_rise_set_transit_spa(
        times=pd.DatetimeIndex([iso_time]),
        latitude=latitude,
        longitude=longitude,
    )
    print(foo456)


"""
13 12 1 0 2 -1 1 20190925 2053458953  9.2818 20190925 2054119318  11.5333
0
0
Unclassified regions
20190925 2053458953 9.6034489515 20190925 2053521545 11.1197829964 20190925 2054046730 11.5333286451 20190925 2054064248 11.5333286451 20190925 2054079263 11.4414296120 20190925 2054116810 10.8440858974 20190925 2054119318 10.4764897652 20190925 2054116810 9.6953479845 20190925 2054111800 9.4196508854 20190925 2054091775 9.2818023359 20190925 2054076760 9.2818023359 20190925 2053483995 9.5574994350 0
100

13 52 26 0 7 -1 1 20191016 1851472920  142.7737148547 20191016 1851573110  147.8358025219
0
10
School detected with:
Minimum data threshold:  -66.00
Maximum data threshold: (none)
Distance mode: GPS distance
Minimum total school height (meters):   4.00
Minimum candidate length (meters):   1.00
Minimum candidate height (meters):   2.00
Maximum vertical linking distance (meters):   2.00
Maximum horizontal linking distance (meters):  20.00
Minimum total school length (meters):   4.00
AH_School
20191016 1851472920 143.9418889317 20191016 1851472920 144.3312802907 20191016 1851482694 144.3312802907 20191016 1851482945 144.3361476827 20191016 1851482945 145.4945869758 20191016 1851482694 145.4994543678 20191016 1851472920 145.4994543678 20191016 1851472920 145.6941500473 20191016 1851482694 145.6941500473 20191016 1851482945 145.6990174393 20191016 1851482945 146.6676284448 20191016 1851492970 146.6676284448 20191016 1851492970 146.4729327653 20191016 1851502740 146.4729327653 20191016 1851502990 146.4778001573 20191016 1851502990 147.0570198039 20191016 1851522770 147.0570198039 20191016 1851523020 147.0618871959 20191016 1851523020 147.4464111629 20191016 1851533040 147.4464111629 20191016 1851533040 147.2517154834 20191016 1851542805 147.2517154834 20191016 1851543055 147.2565828754 20191016 1851543055 147.4464111629 20191016 1851552820 147.4464111629 20191016 1851553070 147.4512785549 20191016 1851553070 147.8358025219 20191016 1851563090 147.8358025219 20191016 1851563090 147.6411068424 20191016 1851573110 147.6411068424 20191016 1851573110 147.0570198039 20191016 1851563090 147.0570198039 20191016 1851563090 146.4729327653 20191016 1851573110 146.4729327653 20191016 1851573110 145.4994543678 20191016 1851563090 145.4994543678 20191016 1851563090 144.7206716498 20191016 1851553070 144.7206716498 20191016 1851553070 143.9418889317 20191016 1851543055 143.9418889317 20191016 1851543055 143.1631062137 20191016 1851533040 143.1631062137 20191016 1851533040 142.9684105342 20191016 1851523020 142.9684105342 20191016 1851523020 142.7737148547 20191016 1851513005 142.7737148547 20191016 1851513005 143.3529345012 20191016 1851512755 143.3578018932 20191016 1851482945 143.3578018932 20191016 1851482945 143.7423258602 20191016 1851482694 143.7471932522 20191016 1851472920 143.7471932522 1
Region 26
"""
#
if __name__ == "__main__":
    try:
        get_solar_azimuth()
        # opened_cruise = open_zarr_store()
        # open_evr_file()  # opened_cruise)
    except Exception as e:
        print(e)
