# ---
# jupyter:
#   jupytext:
#     comment_magics: false
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.3
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Progressive Loading and Visualization
#
# This notebook shows a simple code to download and visualize all the New York Yellow Taxi trips from January 2015, knowing the bounds of NYC.
# The trip data is stored in multiple CSV files, containing geolocated taxi trips.
# We visualize progressively the pickup locations (where people have been picked up by the taxis).

# %%
# We make sure the libraries are reloaded when modified, and avoid warning messages
# %load_ext autoreload
# %autoreload 2
import warnings
warnings.filterwarnings("ignore")

# %%
# Some constants we'll need: the data file to download and final image size
LARGE_TAXI_FILE = "https://www.aviz.fr/nyc-taxi/yellow_tripdata_2015-01.csv.bz2"
RESOLUTION=512

# %% [markdown]
# ## Define NYC Bounds
# If we know the bounds, this will simplify the code.
# See https://en.wikipedia.org/wiki/Module:Location_map/data/USA_New_York_City

# %%
from dataclasses import dataclass
@dataclass
class Bounds:
    top: float = 40.92
    bottom: float = 40.49
    left: float = -74.27
    right: float = -73.68

bounds = Bounds()

# %% [markdown]
# ## Create Modules
# First, create the four modules we need.

# %%
from progressivis import CSVLoader, Histogram2D, ConstDict, Heatmap, PDict

# Create a CSVLoader module, two min/max constant modules, a Histogram2D module, and a Heatmap module.

csv = CSVLoader(LARGE_TAXI_FILE, usecols=['pickup_longitude', 'pickup_latitude'])
min = ConstDict(PDict({'pickup_longitude': bounds.left, 'pickup_latitude': bounds.bottom}))
max = ConstDict(PDict({'pickup_longitude': bounds.right, 'pickup_latitude': bounds.top}))
histogram2d = Histogram2D('pickup_longitude', 'pickup_latitude', xbins=RESOLUTION, ybins=RESOLUTION)
heatmap = Heatmap()

# %% [markdown]
# ## Connect Modules
#
# Then, connect the modules.

# %%
histogram2d.input.table = csv.output.result
histogram2d.input.min = min.output.result
histogram2d.input.max = max.output.result
heatmap.input.array = histogram2d.output.result

# %% [markdown]
# ## Display the Heatmap

# %%
heatmap.display_notebook()

# %% [markdown]
# ## Start the scheduler

# %%
csv.scheduler.task_start()

# %% [markdown]
# ## Show the modules
# printing the scheduler shows all the modules and their states

# %%
csv.scheduler

# %% [markdown]
# ## Stop the scheduler
# To stop the scheduler, uncomment the next cell and run it

# %%

# csv.scheduler.task_stop()

# %%
