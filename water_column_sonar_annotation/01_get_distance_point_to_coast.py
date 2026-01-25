import geopandas as gpd

"""
Gets the distance between a point and a coastline
https://www.kaggle.com/code/notcostheta/shortest-distance-to-a-coastline
https://www.naturalearthdata.com/downloads/50m-physical-vectors/50m-coastline/
"""


def open_shape_file():
    try:
        print("test")
        world = gpd.read_file()
        country = world[world.name == "Australia"]
        print(country)
    except Exception as e:
        print(f"Could not process cruise: {e}")
    print("done")


if __name__ == "__main__":
    open_shape_file()
