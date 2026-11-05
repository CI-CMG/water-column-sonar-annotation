# Water Column Sonar Annotation

Tool for converting EVR files to annotated regions of interest in parquet format

![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/CI-CMG/water-column-sonar-annotation/test_action.yaml)
![PyPI - Version](https://img.shields.io/pypi/v/water-column-sonar-annotation) ![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/CI-CMG/water-column-sonar-annotation) ![GitHub repo size](https://img.shields.io/github/repo-size/CI-CMG/water-column-sonar-annotation)

# Echoview documentation

https://support.echoview.com/WebHelp/Reference/File_Formats/Export_File_Formats/2D_Region_definition_file_format.htm

# Annotations Download

Annotations are available at the first tagged version of this repo:
https://github.com/CI-CMG/water-column-sonar-annotation/releases/tag/v26.1.0

# Setting up the Python Environment

> Python 3.12.12

# Installing Dependencies

```
source .venv/bin/activate

uv pip install --upgrade pip

uv pip install -r pyproject.toml --all-extras

uv run pre-commit install
```

# Pytest

```
uv run pytest tests -W ignore::DeprecationWarning
```

or
> uv run pytest tests/cruise --cov=water_column_sonar_annotation --cov-report term-missing

Current Status:

```
==================================================================== tests coverage =====================================================================
___________________________________________________ coverage: platform darwin, python 3.13.9-final-0 ____________________________________________________

Name                                                                 Stmts   Miss  Cover   Missing
--------------------------------------------------------------------------------------------------
water_column_sonar_annotation/__init__.py                                3      0   100%
water_column_sonar_annotation/astronomical/__init__.py                   2      0   100%
water_column_sonar_annotation/astronomical/astronomical_manager.py      27     18    33%   13-18, 35-43, 57-70
water_column_sonar_annotation/cruise/__init__.py                         2      0   100%
water_column_sonar_annotation/cruise/cruise_manager.py                  63     14    78%   37-38, 45-46, 57-58, 83-84, 100-101, 113-114, 132-133
water_column_sonar_annotation/geospatial/__init__.py                     2      0   100%
water_column_sonar_annotation/geospatial/geospatial_manager.py          56     36    36%   38-40, 44, 54-55, 65-83, 93-100, 108-115, 123-128
water_column_sonar_annotation/record/__init__.py                         4      0   100%
water_column_sonar_annotation/record/echoview_record_manager.py        176    142    19%   38-39, 52-54, 61-93, 96-97, 100, 131, 139-141, 151-155, 162-384, 391-403, 412-456, 462-469
water_column_sonar_annotation/record/graph_record_manager.py            35     29    17%   34-58, 70-73, 80-83
water_column_sonar_annotation/record/parquet_record_manager.py          44     35    20%   46-72, 138-141, 146-149
water_column_sonar_annotation/shape/__init__.py                          2      2     0%   1-3
water_column_sonar_annotation/shape/shape_manager.py                     0      0   100%
--------------------------------------------------------------------------------------------------
TOTAL                                                                  416    276    34%
===== 7 passed in 43.97s ======
```

```
uv run pre-commit install --allow-missing-config
# or
uv run pre-commit install
```

# Test & Coverage

TODO

# Tag a Release

Step 1 --> increment the semantic version in the zarr_manager.py "metadata" & the "pyproject.toml"

```commandline
git tag -a v26.3.1 -m "Releasing v26.3.1"
git push origin --tags
#gh release create v26.2.5
```

# To Publish To PROD

```commandline
uv build --no-sources
uv publish
```

# UV Debugging

```
uv lock --check
uv lock
uv sync --extra dev
#uv run pytest tests
```

## Annotation format

- https://roboflow.com/formats/coco-json
- https://www.v7labs.com/blog/coco-dataset-guide

```json
{
  "id": "8eca84de-2d78-4f62-be35-eccb579d0183",
  "version": 0,
  "name": "test"
}
```