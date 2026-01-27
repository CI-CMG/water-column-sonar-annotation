import itertools
from os import listdir
from os.path import isfile, join
from pathlib import Path

import numpy as np
import pandas as pd

"""
Documentation for echoview record files in EVR format:
https://support.echoview.com/WebHelp/Reference/File_Formats/Export_File_Formats/2D_Region_definition_file_format.htm
"""


def chunks(lst, n):
    """Yield strings from n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        # yield lst[i:i + n]
        yield " ".join(lst[i : i + n])


class EchoviewRecordManager:
    def __init__(
        self,
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
            "Unclassified regions",  # TODO: per CWB continue to include this
            "krill_schools",  # TODO: exclude
            "AH_School",
        ]
        self.all_records_df = pd.DataFrame(columns=["A", "B", "C"])

    def __enter__(self):
        print("__enter__ called")
        return self

    def __exit__(self, *a):
        print("__exit__ called")

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
    """

    # TODO:
    #  [1] write the records to a pandas dataframe
    #  [2] write df to parquet and tag as github resource

    @staticmethod
    def process_datetime_string(
        date_string: str,
        time_string: str,
    ):
        """Returns time in UTC from strings '20190925' and '2053458953'"""
        return pd.to_datetime(f"{date_string} {time_string}", format="%Y%m%d %H%M%S%f")

    def process_vertice(
        self,
        date_string: str,
        time_string: str,
        depth: float,
    ) -> tuple:
        dt = self.process_datetime_string(date_string, time_string)
        print(dt.value)  # is epoch time in nanoseconds
        return dt, dt.value, np.round(depth, 2)

    def process_evr_record(
        self,
        evr_record: str,
        filename: str,
    ):
        try:
            ####################################
            record_lines = [x for x in evr_record.split("\n") if x]
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
            # _evr_region_notes = # Notes associated with the region. Maximum length is 2048 characters. Embedded CR characters are encoded as hexadecimal FF. Embedded LF characters are encoded as hexadecimal FE.
            # _evr_number_of_lines_of_detection_settings = # The number of lines of detection settings to follow.
            # _evr_region_detection_settings = # The detection settings as defined in the Fish Track Detection Properties dialog box or Detect Schools dialog box.
            # _evr_region_classification = # Region classification (string). Default value is "Unclassified regions"
            # _evr_points = # Data for first point – See Data formats below. These data are used to bound the region when importing into Echoview
            # _evr_region_type = # "0" = bad (no data); "1" = analysis; "2" = marker, "3" = fishtracks; "4" = bad (empty water);
            # _evr_region_name = # String
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
                evr_left_x_value_of_bounding_rectangle = self.process_datetime_string(
                    bbox_split[7], bbox_split[8]
                )
                # Upper depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
                evr_top_y_value_of_bounding_rectangle = float(bbox_split[9])
                # Date and time of right boundary of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
                evr_right_x_value_of_bounding_rectangle = self.process_datetime_string(
                    bbox_split[10], bbox_split[11]
                )
                # Lower depth coordinate of bounding rectangle – ignored when importing into Echoview. See "Point 1" in table below.
                evr_bottom_y_value_of_bounding_rectangle = float(bbox_split[12])
                print(evr_left_x_value_of_bounding_rectangle)
                print(evr_top_y_value_of_bounding_rectangle)
                print(evr_right_x_value_of_bounding_rectangle)
                print(evr_bottom_y_value_of_bounding_rectangle)
                # making sure times are in-order
                if (
                    evr_left_x_value_of_bounding_rectangle
                    > evr_right_x_value_of_bounding_rectangle
                ):
                    raise Exception("Timestamps out of order!")
            #
            offset_index = 0
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
            evr_point_chunks = list(itertools.batched(evr_points, 3))
            for evr_point_chunk in evr_point_chunks:
                processed_point = self.process_vertice(
                    date_string=evr_point_chunk[0],
                    time_string=evr_point_chunk[1],
                    depth=float(evr_point_chunk[2]),
                )
                print(processed_point)
            #
            if len(evr_points) != evr_point_count * 3:
                raise Exception("EVR point count does not match expected.")
            #
            # "0" = bad (no data); "1" = analysis; "2" = marker, "3" = fishtracks; "4" = bad (empty water);
            evr_region_type = [x for x in record_lines[-2].split(" ") if x][-1]
            print(f"Region type: {self.region_type[evr_region_type]}")
            # String
            evr_region_name = record_lines[-1]
            print(f"Region name: {evr_region_name}")
            #
            print(f"Filename: {filename}")
            # TODO:
            # start_time
            # end_time
            # min_depth
            # max_depth
            # polygon????
            # altitude
            # latitude
            # longitude
            # ship
            # cruise
            # sensor
            # local time
            # distance from shore
            # solar azimuth
            # is_daytime
            # int month
            # provenance
            #
            print("______________________________________done reading_+_+_+_+_+_+_+_+")
        except Exception as process_evr_record_exception:
            print(f"Problem with process_evr_record: {process_evr_record_exception}")

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
                self.process_evr_record(evr_record=evr_record, filename=evr_filename)
        except Exception as process_evr_file_exception:
            print(
                f"Problem processing file {evr_filename}: {process_evr_file_exception}"
            )

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
        except Exception as process_evr_directory_exception:
            print(
                f"Problem processing evr directory: {process_evr_directory_exception}"
            )


# if __name__ == "__main__":
#     try:
#         echoview_record_manager = EchoviewRecordManager()
#         echoview_record_manager.process_evr_directory(
#             evr_directory_path="../../data/HB201906/"
#         )
#     except Exception as e:
#         print(e)


# Example of polygon
# 20191106 1314583780 25.4929369108 # top-left
# 20191106 1314583780 30.2941528987 # bottom-left
# 20191106 1314593790 30.2941528987 # bottom-right
# 20191106 1314593790 25.3008882713 # top-right
# 20191106 1314583780 25.3008882713 1 # top-left'ish, ends with '1' ...goes counter-clockwise
