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
# # Progressive Loading and Visualization with Issues
#
# This notebook shows a simple code to download and visualize all the New York Yellow Taxi trips from January 2015. 
# The trip data is stored in multiple CSV files, containing geolocated taxi trips.
# We visualize progressively the pickup locations (where people have been picked up by the taxis).
# Unfortunately, with big data, unexpected results can happen.

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
# ## Create Modules
# First, create the four modules we need.

# %%
from progressivis import CSVLoader, Histogram2D, Min, Max, Heatmap

# We will use these two columns
col_x = "pickup_longitude"
col_y = "pickup_latitude"

# Create a CSVLoader module, a Min and Max module, a Histogram2D module, and a Heatmap module.

# The CSV Loader only loads two columns of interest here with the 'usecols' keyword.
csv = CSVLoader(LARGE_TAXI_FILE, usecols=[col_x, col_y])
min = Min(name="min")
max = Max(name="max")
# This Histogram2D column will compute a 2D histogram from the 2 columns with a resolution
histogram2d = Histogram2D(col_x, col_y, xbins=RESOLUTION, ybins=RESOLUTION)
heatmap = Heatmap()


# %% [markdown]
# ## Connect Modules
#
# Then, connect the modules.

# %%
# Now, connect the modules to create the Dataflow graph
# Min/Max input a table and output the min/max values of all the numeric columns
min.input.table = csv.output.result
max.input.table = csv.output.result
histogram2d.input.table = csv.output.result
histogram2d.input.min = min.output.result
histogram2d.input.max = max.output.result
# Connect the Histogram2D to the Heatmap module
heatmap.input.array = histogram2d.output.result

# %% [markdown]
# ## Display the Heatmap
#
# The displayed image only shows two small points instead of the revealing the map of Manhattan.
# This is because a few taxi trips go to Florida and other far away locations with their meter on. The image is thus scaled down to show most of the US instead of Manhattan only.

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
# ## Modify the Dataflow
# To change the program, adding and removing modules, use a context manager

# %%
from progressivis import Quantiles

with csv.scheduler as dataflow:
    quant2 = Quantiles()
    hist2 = Histogram2D(col_x, col_y,
            xbins=RESOLUTION, ybins=RESOLUTION)
    heat2 = Heatmap()
    quant2.input.table = csv.output.result
    hist2.input.table = quant2.output.table
    hist2.input.min = quant2.output.result[0.03]
    hist2.input.max = quant2.output.result[0.97]
    heat2.input.array = hist2.output.result
    deps = dataflow.collateral_damage(min, max)
    print("Will remove modules:", deps)
    dataflow.delete_modules(*deps)


# %%
heat2.display_notebook()

# %%
csv.scheduler

# %% [markdown]
# ## Stop the scheduler
# To stop the scheduler, uncomment the next cell and run it

# %%

# csv.scheduler.task_stop()
