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
git tag -a v26.2.5 -m "Releasing v26.2.5"
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