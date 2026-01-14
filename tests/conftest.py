from pathlib import Path

import pooch
import pytest

HERE = Path(__file__).parent.absolute()
TEST_DATA_FOLDER = HERE / "test_resources"

HB1906_DATA = pooch.create(
    path=pooch.os_cache("water-column-sonar-annotation"),
    base_url="https://github.com/CI-CMG/water-column-sonar-annotation/releases/tag/v26.1.0",
    retry_if_failed=1,
    registry={
        "HB201906_BOTTOMS.zip": "sha256:20609581493ea3326c1084b6868e02aafbb6c0eae871d946f30b8b5f0e7ba059",
        "HB201906_EVR.zip": "sha256:ceed912a25301be8f1b8f91e134d0ca4cff717f52b6623a58677832fd60c2990",
    },
)


def fetch_raw_files():
    HB1906_DATA.fetch(fname="HB201906_BOTTOMS.zip", progressbar=True)
    HB1906_DATA.fetch(fname="HB201906_EVR.zip", progressbar=True)

    file_name = HB1906_DATA.fetch(fname="HB201906_EVR.zip", progressbar=True)

    return Path(file_name).parent


@pytest.fixture(scope="session")
def test_path():
    return {
        "DATA_TEST_PATH": TEST_DATA_FOLDER / "data",
    }


# """
# Folder locations in mac and windows:
#
# Windows
# C:\Users\<user>\AppData\Local\echopype\Cache\2024.12.23.10.10
#
# MacOS
# /Users//Library/Caches/echopype/2024.12.23.10.10
# """
