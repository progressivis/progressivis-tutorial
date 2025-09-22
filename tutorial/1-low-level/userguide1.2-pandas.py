# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.7
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Non Progressive Loading and Visualization
#
# This notebook shows the simplest code to download all the New York Yellow Taxi trips from 2015. They were all geolocated and the trip data is stored in multiple CSV files.
# We visualize progressively the pickup locations (where people have been picked up by the taxis).
#
# First, we define a few constants, where the file is located, the desired resolution, and the url of the taxi file.

# %%
import warnings
warnings.filterwarnings("ignore")
LARGE_TAXI_FILE = "https://www.aviz.fr/nyc-taxi/yellow_tripdata_2015-01.csv.bz2"
RESOLUTION=512


# %%
# See https://en.wikipedia.org/wiki/Module:Location_map/data/USA_New_York_City
from dataclasses import dataclass
@dataclass
class Bounds:
    top: float = 40.92
    bottom: float = 40.49
    left: float = -74.27
    right: float = -73.68

bounds = Bounds()


# %%
import pandas as pd


# Create a csv loader filtering out data outside NYC
# %time
df = pd.read_csv(LARGE_TAXI_FILE, index_col=False, usecols=['pickup_longitude', 'pickup_latitude'])


# %%

# Since there are outliers in the files.
def filter_(df):
    lon = df['pickup_longitude']
    lat = df['pickup_latitude']
    return df[
        (lon > bounds.left) &
        (lon < bounds.right) &
        (lat > bounds.bottom) &
        (lat < bounds.top)
    ]
df = filter_(df)


# %%
import matplotlib.pyplot as plt

# %%
plt.hist2d(df.pickup_longitude, df.pickup_latitude, bins=(512, 512), norm="symlog", cmap=plt.cm.Greys_r)
plt.colorbar()
plt.show()
