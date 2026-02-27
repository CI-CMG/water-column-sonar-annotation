import hashlib
import itertools
import os
import zipfile
from pathlib import Path

import pandas as pd
import pooch

from water_column_sonar_annotation.astronomical import AstronomicalManager
from water_column_sonar_annotation.cruise import CruiseManager
from water_column_sonar_annotation.geospatial import GeospatialManager
from water_column_sonar_annotation.record.parquet_record_manager import (
    ParquetRecordManager,
)

"""
Documentation for echoview record files in EVR format:
https://support.echoview.com/WebHelp/Reference/File_Formats/Export_File_Formats/2D_Region_definition_file_format.htm
"""

HB201906_EVR = pooch.create(
    path=pooch.os_cache("water-column-sonar-annotation"),
    base_url="https://github.com/CI-CMG/water-column-sonar-annotation/releases/download/v26.1.0/",
    retry_if_failed=1,
    registry={
        # "HB201906_BOTTOMS.zip": "sha256:20609581493ea3326c1084b6868e02aafbb6c0eae871d946f30b8b5f0e7ba059",
        "HB201906_EVR.zip": "sha256:256778122e9c8b05884f5f2c5cf5cdad5502aa3dae61d417ec5e4278262d937a",
    },
)


def fetch_raw_files():
    # HB201906_EVR.fetch(fname="HB201906_BOTTOMS.zip", progressbar=True)
    file_path = HB201906_EVR.fetch(fname="HB201906_EVR.zip", progressbar=True)
    if not os.path.isdir(os.path.join(Path(file_path).parent, "HB201906_EVR")):
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(os.path.join(Path(file_path).parent, "HB201906_EVR"))

    return os.path.join(Path(file_path).parent, "HB201906_EVR")


def data_path():
    return {
        "DATA_PATH": fetch_raw_files(),
    }


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
        self.evr_region_classifications = [  # Specific to Mike's workflow
            "possible_herring",
            "atlantic_herring",
            "fish_school",
            "Unclassified regions",  # TODO: per CWB continue to include this
            "krill_schools",  # excluding this field because of unknowns
            "AH_School",
        ]
        self.all_records_df = pd.DataFrame()  # columns=["filename", "start_time"])
        self.astronomical_manager = AstronomicalManager()
        self.cruise_manager = CruiseManager()
        self.geospatial_manager = GeospatialManager()

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

    @staticmethod
    def process_datetime_string(
        date_string: str,
        time_string: str,
    ):
        """Returns time in UTC from strings '20190925' and '2053458953'"""
        # np.datetime64()
        return pd.to_datetime(f"{date_string} {time_string}", format="%Y%m%d %H%M%S%f")

    def process_vertice(
        self,
        date_string: str,
        time_string: str,
        depth: float,
    ) -> tuple:
        dt = self.process_datetime_string(date_string, time_string)
        # print(dt.isoformat())  # is epoch time in nanoseconds
        return dt.value, round(depth, 2)

    def process_vertice_index(
        self,
        time,  # nanoseconds
        depth,
    ) -> tuple:
        # Takes time/depth and returns the associated indexes from the cruise
        #  used by leaflet coordinates to draw polygons
        #  need to get cruise and convert time to nearest ping_time
        time_index = self.cruise_manager.get_time_index(time)
        # get cruise and convert depth to nearest depth
        depth_index = self.cruise_manager.get_depth_index(depth)
        # Note: depth needs to be negative for leaflet but keep pos here
        return time_index, depth_index

    def process_evr_record_full_geometry(
        self,
        evr_record: str,
        file_name: str,
    ):
        try:
            #########################################################
            record_lines = [x for x in evr_record.split("\n") if x]
            ############# get bbox #############
            bbox_split = record_lines[0].split()  # [x for x in record.split() if x]
            #########################################################
            # https://support.echoview.com/WebHelp/Reference/File_Formats/Export_File_Formats/2D_Region_definition_file_format.htm
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
                f"EVR region creation type: {self.region_creation_type[evr_region_creation_type]}"
            )
            #
            evr_dummy = bbox_split[5]  # Should always be "-1"
            if evr_dummy != "-1":
                raise Exception("EVR Dummy Should always be -1")
            #
            ### "1" if the next four fields are valid; "0" otherwise ###
            evr_bounding_rectangle_calculated = bbox_split[6]
            evr_left_x_value_of_bounding_rectangle = None
            evr_top_y_value_of_bounding_rectangle = None
            evr_right_x_value_of_bounding_rectangle = None
            evr_bottom_y_value_of_bounding_rectangle = None
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
                print(
                    f"{evr_left_x_value_of_bounding_rectangle.isoformat()}, {evr_top_y_value_of_bounding_rectangle}, {evr_right_x_value_of_bounding_rectangle.isoformat()}, {evr_bottom_y_value_of_bounding_rectangle}"
                )
                # making sure times are in-order
                if (
                    evr_left_x_value_of_bounding_rectangle
                    > evr_right_x_value_of_bounding_rectangle
                ):
                    raise Exception("Timestamps out of order!")
            #
            offset_index = 0
            ### The number of lines of region notes to follow. ###
            evr_number_of_lines_of_notes = int(record_lines[1])
            print(f"Number of region notes: {evr_number_of_lines_of_notes}")
            ### Notes associated with the region. Maximum length is 2048 characters. Embedded CR characters are encoded as hexadecimal FF. Embedded LF characters are encoded as hexadecimal FE. ###
            if evr_number_of_lines_of_notes > 0:
                offset_index = offset_index + evr_number_of_lines_of_notes + 1
                evr_region_notes = record_lines[1:offset_index]
                print(f"Region notes: {evr_region_notes}")
            #
            ### The number of lines of detection settings to follow. ###
            evr_number_of_lines_of_detection_settings = int(
                record_lines[2 + offset_index]
            )
            print(
                f"Number of lines of detection settings: {evr_number_of_lines_of_detection_settings}"
            )
            ### The detection settings as defined in the Fish Track Detection Properties dialog box or Detect Schools dialog box. ###
            if evr_number_of_lines_of_detection_settings > 0:
                offset_index = (
                    evr_number_of_lines_of_notes
                    + evr_number_of_lines_of_detection_settings
                    + 3
                )
                evr_region_detection_settings = record_lines[3:offset_index]
                print(f"Region detection settings: {evr_region_detection_settings}")
            #
            ### Region classification (string). Default value is "Unclassified regions" ###
            evr_region_classification = record_lines[-3]
            if evr_region_classification not in self.evr_region_classifications:
                print(f"Unknown region classification: {evr_region_classification}")
            print(f"Region classification: {evr_region_classification}")
            #
            # TODO: If the data has krill, skip creating a record of it
            if evr_region_classification == "krill_schools":
                print("Krill, skipping!!!")
                return None
            #
            # Data for first point – See Data formats below. These data are used to bound the region when importing into Echoview
            evr_points = [x for x in record_lines[-2].split(" ") if x][:-1]
            # print(f"EVR points: {evr_points}")  # TODO: strip last entry
            #
            evr_point_chunks = list(itertools.batched(evr_points, 3))
            #########################################################
            #########################################################
            # TODO: these should be in the cruise index coordinates as well for leaflet
            x_times = []
            y_depths = []
            x_index_times = []
            y_index_depths = []
            for evr_point_chunk in evr_point_chunks:
                processed_point = self.process_vertice(
                    date_string=evr_point_chunk[0],
                    time_string=evr_point_chunk[1],
                    depth=float(evr_point_chunk[2]),
                )
                x_times.append(processed_point[0])
                y_depths.append(processed_point[1])
                #
                processed_index_point = self.process_vertice_index(
                    time=processed_point[0] / 1000,
                    depth=processed_point[1],
                )
                x_index_times.append(processed_index_point[0])
                y_index_depths.append(processed_index_point[1])

            #########################################################
            #########################################################
            #
            if len(x_times) != evr_point_count:
                raise Exception("EVR point count does not match expected.")
            #
            # "0" = bad (no data); "1" = analysis; "2" = marker, "3" = fishtracks; "4" = bad (empty water);
            evr_region_type = [x for x in record_lines[-2].split(" ") if x][-1]
            print(f"Region type: {self.region_type[evr_region_type]}")
            # String
            evr_region_name = record_lines[-1]
            print(f"Region name: {evr_region_name}")
            #
            print("get lat lon")
            (latitude, longitude) = self.cruise_manager.get_coordinates(
                start_time=evr_left_x_value_of_bounding_rectangle.isoformat(),
                end_time=evr_right_x_value_of_bounding_rectangle.isoformat(),
            )
            print("get local time")
            local_time = self.geospatial_manager.get_local_time(
                iso_time=evr_left_x_value_of_bounding_rectangle.isoformat(),
                latitude=latitude,
                longitude=longitude,
            )
            print("get solar")
            solar_altitude = self.astronomical_manager.get_solar_azimuth(
                iso_time=evr_left_x_value_of_bounding_rectangle.isoformat(),
                latitude=latitude,
                longitude=longitude,
            )
            print("phase_of_day")
            phase_of_day = self.astronomical_manager.phase_of_day(
                iso_time=evr_left_x_value_of_bounding_rectangle.isoformat(),
                latitude=latitude,
                longitude=longitude,
            )
            print("distance")
            distance_from_coastline = (  # Note this takes about 14 seconds each, very slow
                self.geospatial_manager.check_distance_from_coastline(
                    latitude=latitude,
                    longitude=longitude,
                )
            )
            print("altitude")
            evr_altitude = self.cruise_manager.get_altitude(
                start_time=evr_left_x_value_of_bounding_rectangle.isoformat(),
                end_time=evr_right_x_value_of_bounding_rectangle.isoformat(),
                bbox_max=evr_bottom_y_value_of_bounding_rectangle,
            )
            #
            ### provenance ###
            geometry_string = record_lines[-2]  # inclusive of evr_region_type
            geometry_hash = hashlib.sha256(geometry_string.encode("utf-8")).hexdigest()
            #
            parquet_record_manager = ParquetRecordManager(
                ###
                time_start=evr_left_x_value_of_bounding_rectangle,
                time_end=evr_right_x_value_of_bounding_rectangle,
                depth_min=evr_top_y_value_of_bounding_rectangle,
                depth_max=evr_bottom_y_value_of_bounding_rectangle,
                altitude=evr_altitude,
                latitude=latitude,
                longitude=longitude,
                distance_from_coastline=distance_from_coastline,
                local_time=local_time,
                month=evr_left_x_value_of_bounding_rectangle.month,
                solar_altitude=solar_altitude,
                phase_of_day=phase_of_day,
                classification=evr_region_classification,
                filename=file_name,
                region_id=evr_region_id,
                ship="Henry_B._Bigelow",
                cruise="HB1906",
                instrument="EK60",
                point_count=evr_point_count,
                geometry_hash=geometry_hash,
                x=x_times,
                y=y_depths,
                x_index=x_index_times,
                y_index=y_index_depths,
            )
            update_df = pd.DataFrame([parquet_record_manager.to_dict()])
            self.all_records_df = pd.concat(
                [self.all_records_df, update_df],
                ignore_index=True,
            )
            return parquet_record_manager
        except Exception as process_evr_record_exception:
            print(f"Problem with process_evr_record: {process_evr_record_exception}")
        finally:
            print("______________________________________done reading_+_+_+_+_+_+_+_+")

    def process_evr_file(
        self,
        evr_file_path: str = None,
        evr_file_name: str = None,
    ):
        try:
            print(f"Filename: {evr_file_name}")
            with open(os.path.join(evr_file_path, evr_file_name), "r") as file:
                lines = file.read()

            records = lines.split("\n\n")
            records = [i for i in records if i.startswith("13 ")]  # filter
            for evr_record in records:
                self.process_evr_record_full_geometry(
                    evr_record=evr_record, file_name=evr_file_name
                )
        except Exception as process_evr_file_exception:
            print(
                f"Problem processing file {evr_file_name}: {process_evr_file_exception}"
            )

    def process_evr_directory(
        self,
        evr_directory_path: str = data_path()["DATA_PATH"],
    ):
        """Open evr directory and start to parse files"""
        try:
            all_evr_files = [
                os.path.join(dp, f)
                for dp, dn, filenames in os.walk(evr_directory_path)
                for f in filenames
                if (os.path.splitext(f)[1] == ".evr")
            ]
            all_evr_files.sort()
            print(f"Found {len(all_evr_files)} EVR files.")
            #
            # TODO: only processing two files right now
            #
            for evr_file in all_evr_files:
                self.process_evr_file(
                    evr_file_path=os.path.dirname(evr_file),
                    evr_file_name=os.path.basename(evr_file),
                )
            # I don't have the lat/lon information to draw here... need to query the zarr store...
            print(self.all_records_df)
            # self.all_records_df.set_index(
            #     keys="geometry_hash", drop=False, inplace=True
            # )
            #  sort by time
            self.all_records_df.sort_values(
                by="time_start",
                axis=0,
                ascending=True,
                inplace=True,
                ignore_index=False,
            )
            print("writing files")
            #
            # for front-end visualization
            self.all_records_df.to_parquet(
                path="parquet_record_full.parquet",
                engine="pyarrow",
                compression="snappy",
                index=True,
                # partition_cols=None,
            )
            # TODO: omit geometry and write to csv for upload to neo4j
            # self.all_records_df.to_csv(
            #     path_or_buf="parquet_record_full_geometry.csv",
            #     header=True,
            #     index=False,
            #     mode="w",
            # )
            print("done writing files")
            #
        except Exception as process_evr_directory_exception:
            print(
                f"Problem processing evr directory: {process_evr_directory_exception}"
            )


# if __name__ == "__main__":
#     try:
#         echoview_record_manager = EchoviewRecordManager()
#         echoview_record_manager.process_evr_directory()
#         print("done processing everything")
#     except Exception as e:
#         print(e)


# Example of polygon
# 20191106 1314583780 25.4929369108 # top-left
# 20191106 1314583780 30.2941528987 # bottom-left
# 20191106 1314593790 30.2941528987 # bottom-right
# 20191106 1314593790 25.3008882713 # top-right
# 20191106 1314583780 25.3008882713 1 # top-left'ish, ends with '1' ...goes counter-clockwise
