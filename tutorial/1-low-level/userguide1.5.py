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
from progressivis import (
    CSVLoader,
    Print,
    Min, Max,
    Heatmap,
    get_dataset,
)

# %%
import warnings
warnings.filterwarnings("ignore")
LARGE_TAXI_FILE = "https://www.aviz.fr/nyc-taxi/yellow_tripdata_2015-01.csv.bz2"

def terse(x):
    print(".", end="", flush=True)

def terse2(x):
    print("/", end="", flush=True)


# %%
csv = CSVLoader(LARGE_TAXI_FILE, usecols=['pickup_longitude', 'pickup_latitude'])
m = Min()
prt = Print(proc=terse)
m.input.table = csv.output.result
prt.input.df = m.output.result

# %%
csv.scheduler.task_start()

# %%
csv.scheduler

# %%
with csv.scheduler as dataflow:
    M = Max()
    prt2 = Print(proc=terse2)
    M.input.table = csv.output.result
    prt2.input.df = M.output.result

# %%
csv.scheduler

# %%
csv.scheduler.task_stop()

# %%
