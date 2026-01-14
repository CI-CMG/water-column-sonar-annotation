# water-column-sonar-annotation
Tool for converting EVR files to annotated regions of interest using semantic segmentation

# Setting up the Python Environment
> Python 3.12.9

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
> pytest --cache-clear --cov=src tests/ --cov-report=xml

# Instructions
Following this tutorial:
https://packaging.python.org/en/latest/tutorials/packaging-projects/

# Pre Commit Hook
see here for installation: https://pre-commit.com/
https://dev.to/rafaelherik/using-trufflehog-and-pre-commit-hook-to-prevent-secret-exposure-edo
```
uv run pre-commit install --allow-missing-config
# or
uv run pre-commit install
```

# Colab Test
https://colab.research.google.com/drive/1KiLMueXiz9WVB9o4RuzYeGjNZ6PsZU7a#scrollTo=AayVyvpBdfIZ

# Test Coverage
TODO

# Tag a Release

Step 1 --> increment the semantic version in the zarr_manager.py "metadata" & the "pyproject.toml"

```commandline
git tag -a v26.1.0 -m "Releasing v26.1.0"
git push origin --tags
gh release create v26.1.0
```

# To Publish To PROD
```commandline
uv build --no-sources
uv publish
```

# TODO:
add https://pypi.org/project/setuptools-scm/
for extracting the version

# Security scanning
> bandit -r water_column_sonar_processing/

# Data Debugging
Experimental Plotting in Xarray (hvPlot):
https://colab.research.google.com/drive/18vrI9LAip4xRGEX6EvnuVFp35RAiVYwU#scrollTo=q9_j9p2yXsLV

HB0707 Zoomable Cruise:
https://hb0707.s3.us-east-1.amazonaws.com/index.html


# UV Debugging
```
uv lock --check
uv lock
uv sync --extra dev
#uv run pytest tests
```

# Colab Test
https://colab.research.google.com/drive/1KiLMueXiz9WVB9o4RuzYeGjNZ6PsZU7a#scrollTo=AayVyvpBdfIZ


# Data Debugging
Experimental Plotting in Xarray (hvPlot):
https://colab.research.google.com/drive/18vrI9LAip4xRGEX6EvnuVFp35RAiVYwU#scrollTo=q9_j9p2yXsLV

HB0707 Cruise zoomable:
https://hb0707.s3.us-east-1.amazonaws.com/index.html

## Annotation format
- https://roboflow.com/formats/coco-json
- https://www.v7labs.com/blog/coco-dataset-guide
