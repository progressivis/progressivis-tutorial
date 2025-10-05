# ---
# jupyter:
#   jupytext:
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

# %%

# %% [markdown]
# # Progressive Loading and Visualization
#
# This notebook shows a simple code to download all the New York Yellow Taxi trips from 2015 along with a progress bar and quality feedback.
# We visualize progressively the pickup locations (where people have been picked up by the taxis).
#
# First, we define a few constants, where the file is located, the desired resolution, and the url of the taxi file.

# %%
LARGE_TAXI_FILE = "https://www.aviz.fr/nyc-taxi/yellow_tripdata_2015-01.csv.bz2"
RESOLUTION=512


# %%
# Function to filter out trips outside of NYC.

# See https://en.wikipedia.org/wiki/Module:Location_map/data/USA_New_York_City
bounds = {
    "top": 40.92,
    "bottom": 40.49,
    "left": -74.27,
    "right": -73.68,
}

# %%
from progressivis import CSVLoader, Histogram2D, Min, Max, Heatmap

# Create a csv loader filtering out data outside NYC
csv = CSVLoader(LARGE_TAXI_FILE, usecols=['pickup_longitude', 'pickup_latitude']) #, filter_=filter_)
# Create a module to compute the min value progressively
min = Min()
# Connect it to the output of the csv module
min.input.table = csv.output.result
# Create a module to compute the max value progressively
max = Max()
# Connect it to the output of the csv module
max.input.table = csv.output.result

# Create a module to compute the 2D histogram of the two columns specified
# with the given resolution
histogram2d = Histogram2D('pickup_longitude', 'pickup_latitude', xbins=RESOLUTION, ybins=RESOLUTION)
# Connect the module to the csv results and the min,max bounds to rescale
histogram2d.input.table = csv.output.result
histogram2d.input.min = min.output.result
histogram2d.input.max = max.output.result
# Create a module to create an heatmap image from the histogram2d
heatmap = Heatmap()
# Connect it to the histogram2d
heatmap.input.array = histogram2d.output.result

# %%
heatmap.display_notebook()
# Start the scheduler
csv.scheduler.task_start()

# %%
# Show what runs
csv.scheduler

# %%
import ipywidgets as ipw

from progressivis import Module
from ipyprogressivis.widgets import QualityVisualization

def display_quality(mods, period: float = 3) -> QualityVisualization:
    qv = QualityVisualization()
    last = 0  # show immediately
    if isinstance(mods, Module):
        mods = [mods]

    async def _after_run(m: Module, run_number: int) -> None:
        nonlocal last
        now = m.last_time()
        if (now - last) < period:
            return
        last = now
        measures = m.get_quality()
        if measures is not None:
            qv.update(measures, now)

    for mod in mods:
        mod.on_after_run(_after_run)
    return qv


def display_progress_bar(mod: Module, period: float = 3) -> ipw.IntProgress:
    prog_wg = ipw.IntProgress(
        description="Progress", min=0, max=1000, layout={"width": "200"}
    )

    def _proc(m: Module, r: int) -> None:
        val_, max_ = m.get_progress()
        prog_wg.value = val_
        if prog_wg.max != max_:
            prog_wg.max = max_
    mod.on_after_run(_proc) 
    return prog_wg



# %%
min.get_quality()

# %%
min.timer()

# %%
minq = display_quality(min)
minq

# %%
maxq = display_quality(max)
maxq

# %%
maxq.width = "100%"
maxq.height = 100

# %%
display_progress_bar(min)

# %%
csv.scheduler.task_stop()
