from os import listdir
from os.path import isfile, join
from pathlib import Path

import pandas as pd

"""
Documentation for echoview record EVR files:
https://support.echoview.com/WebHelp/Reference/File_Formats/Export_File_Formats/2D_Region_definition_file_format.htm
"""


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        # yield lst[i:i + n]
        yield " ".join(lst[i : i + n])


class EchoviewRecordManager:
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
        self.evr_region_classifications = [
            "possible_herring",
            "fish_school",
            "Unclassified regions",
            "krill_schools",
            "AH_School",
        ]
        # #
        # self.region_structure_version = None  # "13" (will be incremented if the region structure changes in future versions)
        # self.point_count = None  # Number of points in the region
        # self.region_id = None  # # Unique number for each region. Specify sequential numbers starting at 1 if creating a new file
        # self.selected = None  # "0" (always)
        # self.region_creation_type = None  # See "Data formats" definition
        # self.dummy = None  # Should always be "-1"
        # self.bounding_rectangle_calculated = (
        #     None  # "1" if the next four fields are valid; "0" otherwise
        # )
        # self.left_x_value_of_bounding_rectangle: Optional[str] = (
        #     None  # Date and time of left boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
        # )
        # self.top_y_value_of_bounding_rectangle: Optional[str] = (
        #     None  # Upper depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
        # )
        # self.right_x_value_of_bounding_rectangle: Optional[str] = (
        #     None  # Date and time of right boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
        # )
        # self.bottom_y_value_of_bounding_rectangle: Optional[str] = (
        #     None  # Lower depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
        # )
        # self.number_of_lines_of_notes = (
        #     None  # The number of lines of region notes to follow.
        # )
        # self.region_notes: Optional[str] = (
        #     None  # Notes associated with the region. Maximum length is 2048 characters. Embedded CR characters are encoded as hexadecimal FF. Embedded LF characters are encoded as hexadecimal FE.
        # )
        # self.number_of_lines_of_detection_settings = (
        #     None  # The number of lines of detection settings to follow.
        # )
        # self.region_detection_settings: Optional[str] = (
        #     None  # The detection settings as defined in the Fish Track Detection Properties dialog box or Detect Schools dialog box.
        # )
        # self.region_classification = None  # Region classification (string). Default value is "Unclassified regions"
        # self.points = None  # Data for first point – See Data formats below. These data are used to bound the region when importing into Echoview
        # self.region_type = None  # "0" = bad (no data); "1" = analysis; "2" = marker, "3" = fishtracks; "4" = bad (empty water);
        # self.region_name = None  # String

    def __enter__(self):
        print("__enter__ called")
        return self

    def __exit__(self, *a):
        print("__exit__ called")

    # TODO: maybe remove?
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

    @staticmethod
    def process_temporal_string(
        date_string: str = None,
        time_string: str = None,
    ):
        """Returns time in UTC from ['20190925', '2053458953']"""
        return pd.to_datetime(f"{date_string} {time_string}", format="%Y%m%d %H%M%S%f")

    def process_evr_record(
        self,
        evr_record: str = None,
    ):
        try:
            # print(evr_record)
            record_lines = [x for x in evr_record.split("\n") if x]
            ####################################
            ############# get bbox #############
            bbox_split = record_lines[0].split()  # [x for x in record.split() if x]
            #########################################################
            # https://support.echoview.com/WebHelp/Reference/File_Formats/Export_File_Formats/2D_Region_definition_file_format.htm
            # _evr_region_structure_version = bbox_split[0] # "13" (will be incremented if the region structure changes in future versions)
            # _evr_point_count = bbox_split[1] # Number of points in the region
            # _evr_region_id = # Unique number for each region. Specify sequential numbers starting at 1 if creating a new file
            # _evr_selected = # "0" (always)
            # _evr_region_creation_type = # See "Data formats" definition
            # _evr_dummy = # Should always be "-1"
            # _evr_bounding_rectangle_calculated = # "1" if the next four fields are valid; "0" otherwise
            # _evr_left_x_value_of_bounding_rectangle = # Date and time of left boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
            # _evr_top_y_value_of_bounding_rectangle = # Upper depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
            # _evr_right_x_value_of_bounding_rectangle = # Date and time of right boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
            # _evr_bottom_y_value_of_bounding_rectangle = # Lower depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
            # _evr_number_of_lines_of_notes = # The number of lines of region notes to follow.
            # evr_region_notes = # Notes associated with the region. Maximum length is 2048 characters. Embedded CR characters are encoded as hexadecimal FF. Embedded LF characters are encoded as hexadecimal FE.
            # evr_number_of_lines_of_detection_settings = # The number of lines of detection settings to follow.
            # evr_region_detection_settings = # The detection settings as defined in the Fish Track Detection Properties dialog box or Detect Schools dialog box.
            # evr_region_classification = # Region classification (string). Default value is "Unclassified regions"
            # evr_points = # Data for first point – See Data formats below. These data are used to bound the region when importing into Echoview
            # evr_region_type = # "0" = bad (no data); "1" = analysis; "2" = marker, "3" = fishtracks; "4" = bad (empty water);
            # evr_region_name = # String
            #########################################################
            evr_region_structure_version = bbox_split[0]
            if evr_region_structure_version != "13":
                raise Exception("EVR Region Structure Version must be 13")
            #
            evr_point_count = int(bbox_split[1])
            print(f"EVR Point Count: {evr_point_count}")
            #
            evr_region_id = int(bbox_split[2])
            print(f"EVR Region: {evr_region_id}")
            #
            evr_selected = bbox_split[3]
            if evr_selected != "0":
                raise Exception("EVR Selected must be 13")
            #
            evr_region_creation_type = bbox_split[4]  # See "Data formats" definition
            print(
                f"Region creation type: {self.region_creation_type[evr_region_creation_type]}"
            )
            #
            evr_dummy = bbox_split[5]  # Should always be "-1"
            if evr_dummy != "-1":
                raise Exception("EVR Dummy Should always be -1")
            #
            # "1" if the next four fields are valid; "0" otherwise
            evr_bounding_rectangle_calculated = bbox_split[6]
            if evr_bounding_rectangle_calculated == "1":
                # Date and time of left boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
                # '20190925 2053458953' <-- TODO: format into datetime
                evr_left_x_value_of_bounding_rectangle = self.process_temporal_string(
                    bbox_split[7], bbox_split[8]
                )
                # Upper depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
                evr_top_y_value_of_bounding_rectangle = float(bbox_split[9])
                # Date and time of right boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
                evr_right_x_value_of_bounding_rectangle = self.process_temporal_string(
                    bbox_split[10], bbox_split[11]
                )
                # Lower depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
                evr_bottom_y_value_of_bounding_rectangle = float(bbox_split[12])
                print(evr_left_x_value_of_bounding_rectangle)
                print(evr_top_y_value_of_bounding_rectangle)
                print(evr_right_x_value_of_bounding_rectangle)
                print(evr_bottom_y_value_of_bounding_rectangle)
            # TODO: make sure times are in-order!!!
            offset_index = 0
            #
            # The number of lines of region notes to follow.
            evr_number_of_lines_of_notes = int(record_lines[1])
            print(f"Number of region notes: {evr_number_of_lines_of_notes}")
            # Notes associated with the region. Maximum length is 2048 characters. Embedded CR characters are encoded as hexadecimal FF. Embedded LF characters are encoded as hexadecimal FE.
            if evr_number_of_lines_of_notes > 0:
                offset_index = offset_index + evr_number_of_lines_of_notes + 1
                evr_region_notes = record_lines[1:offset_index]
                print(f"Region notes: {evr_region_notes}")
            #
            # The number of lines of detection settings to follow.
            evr_number_of_lines_of_detection_settings = int(
                record_lines[2 + offset_index]
            )
            print(
                f"Number of lines of detection settings: {evr_number_of_lines_of_detection_settings}"
            )
            # The detection settings as defined in the Fish Track Detection Properties dialog box or Detect Schools dialog box.
            if evr_number_of_lines_of_detection_settings > 0:
                offset_index = (
                    evr_number_of_lines_of_notes
                    + evr_number_of_lines_of_detection_settings
                    + 3
                )
                evr_region_detection_settings = record_lines[3:offset_index]
                print(f"Region detection settings: {evr_region_detection_settings}")
            #
            # Region classification (string). Default value is "Unclassified regions"
            evr_region_classification = record_lines[-3]
            if evr_region_classification not in self.evr_region_classifications:
                raise Exception(
                    f"Problem, unknown region classification: {evr_region_classification}"
                )
            print(f"Region classification: {evr_region_classification}")
            # Data for first point – See Data formats below. These data are used to bound the region when importing into Echoview
            evr_points = [x for x in record_lines[-2].split(" ") if x][:-1]
            print(f"EVR points: {evr_points}")  # TODO: strip last entry
            #
            if len(evr_points) != evr_point_count * 3:
                raise Exception("EVR point count does not match expected.")
            #
            # "0" = bad (no data); "1" = analysis; "2" = marker, "3" = fishtracks; "4" = bad (empty water);
            evr_region_type = [x for x in record_lines[-2].split(" ") if x][-1]
            print(f"Region type: {self.region_type[evr_region_type]}")
            #
            # String
            evr_region_name = record_lines[-1]
            print(f"Region name: {evr_region_name}")
            #
            # upper_left = [
            #     converted_time_start,
            #     float(bbox_split[9]),
            # ]  # [x_min, y_min]
            # bottom_right = [
            #     converted_time_end,
            #     float(bbox_split[12]),
            # ]  # [x_max, y_max]
            # # bounding_box = record_split[0]  # TODO: get box
            # # polygon_label = record_split[-3]
            # print(f"bounding box: {upper_left}, {bottom_right}")
            # ##########################################
            # ############# get data label #############
            # data_label = record_lines[-3]
            # print(data_label)
            # #######################################
            # ############# get polygon #############
            # polygon_data = record_lines[-2]
            # print(polygon_data)
            # #
            # # [1] break up polygon
            # if True:
            #     evr = EchoviewRecordManager()
            #     polygon_region = evr.process_point_data(polygon_data.split(" "))
            #     print(polygon_region)
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
            print("______________________________________done reading")
        except Exception as e:
            print(f"Problem with process_evr_record: {e}")

    def process_evr_file(
        self,
        evr_file_path: str = None,
        evr_filename: str = None,
    ):
        try:
            print(f"Filename: {evr_filename}")
            with open(evr_file_path + evr_filename, "r") as file:
                lines = file.read()

            records = lines.split("\n\n")
            records = [i for i in records if i.startswith("13 ")]  # filter
            for evr_record in records:
                self.process_evr_record(evr_record=evr_record)
        except Exception as e:
            print(e)
        finally:
            print("done processing file")

    def process_evr_directory(self, evr_directory_path="../../data/HB201906/"):
        """Open evr directory and start to parse files"""
        try:
            all_evr_files = [
                f
                for f in listdir(evr_directory_path)
                if isfile(join(evr_directory_path, f)) and Path(f).suffix == ".evr"
            ]
            all_evr_files.sort()
            print(f"Found {len(all_evr_files)} EVR files.")
            for evr_file in all_evr_files:
                self.process_evr_file(
                    evr_file_path=evr_directory_path, evr_filename=evr_file
                )
            # I don't have the lat/lon information to draw here... need to query the zarr store...
        except Exception as e123:
            print(f"Exception encountered processing evr directory: {e123}")
        finally:
            print("done processing evr directory")


labels_with_extra_data = ["fish_school", "krill_schools", "AH_School"]

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
# if __name__ == "__main__":
#     try:
#         echoview_record_manager = EchoviewRecordManager()
#         echoview_record_manager.process_evr_directory(
#             evr_directory_path="../../data/HB201906/"
#         )
#     except Exception as e:
#         print(e)


# 20191106 1314583780 25.4929369108 # top-left
# 20191106 1314583780 30.2941528987 # bottom-left
# 20191106 1314593790 30.2941528987 # bottom-right
# 20191106 1314593790 25.3008882713 # top-right
# 20191106 1314583780 25.3008882713 1 # top-left'ish, ends with '1' ...goes counter-clockwise
