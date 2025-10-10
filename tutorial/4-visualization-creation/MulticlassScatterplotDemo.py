# -*- coding: utf-8 -*-
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

# %%
import warnings
import pandas as pd
from progressivis import Scheduler, Every, PTable, CSVLoader, Constant
from progressivis.vis import MCScatterPlot
from ipyprogressivis.widgets.scatterplot import Scatterplot

warnings.filterwarnings('ignore')  # remove warning messages


def _quiet(x): pass


def _filter(df):
    pklon = df['pickup_longitude']
    pklat = df['pickup_latitude']
    dolon = df['dropoff_longitude']
    dolat = df['dropoff_latitude']

    return df[(pklon > -74.08) & (pklon < -73.5) &
              (pklat > 40.55) & (pklat < 41.00) &
              (dolon > -74.08) & (dolon < -73.5) &
              (dolat > 40.55) & (dolat < 41.00)]


try:
    s = scheduler  # reuse scheduler if it exists
except NameError:
    s = Scheduler()  # create a new Scheduler

PREFIX = 'https://www.aviz.fr/nyc-taxi/'
SUFFIX = '.bz2'
URLS = [
    PREFIX+'yellow_tripdata_2015-01.csv'+SUFFIX,
    PREFIX+'yellow_tripdata_2015-02.csv'+SUFFIX,
    PREFIX+'yellow_tripdata_2015-03.csv'+SUFFIX,
    PREFIX+'yellow_tripdata_2015-04.csv'+SUFFIX,
    PREFIX+'yellow_tripdata_2015-05.csv'+SUFFIX,
    PREFIX+'yellow_tripdata_2015-06.csv'+SUFFIX,
]

FILENAMES = pd.DataFrame({'filename': URLS})
CST = Constant(PTable('filenames', data=FILENAMES), scheduler=s)
CSV = CSVLoader(usecols=['pickup_longitude', 'pickup_latitude',
                         'dropoff_longitude', 'dropoff_latitude'],
                filter_=_filter, scheduler=s)

CSV.input.filenames = CST.output[0]
PR = Every(scheduler=s, proc=_quiet)
PR.input.df = CSV.output[0]

MULTICLASS = MCScatterPlot(
    scheduler=s,
    classes=[
        ('pickup', 'pickup_longitude', 'pickup_latitude'),
        ('dropoff', 'dropoff_longitude', 'dropoff_latitude')
    ],
    approximate=True)
MULTICLASS.create_dependent_modules(CSV, 'result')
sc = Scatterplot()
sc.link_module(MULTICLASS)

# %%
import ipywidgets as ipw


def display_progress_bar(mod, period=3) -> ipw.IntProgress:
    prog_wg = ipw.IntProgress(
        description="Progress", min=0, max=1000, layout={"width": "200"}
    )

    def _proc(m, r) -> None:
        val_, max_ = m.get_progress()
        prog_wg.value = val_
        if prog_wg.max != max_:
            prog_wg.max = max_
    mod.on_after_run(_proc)
    return prog_wg


vbox = ipw.VBox([sc, display_progress_bar(CSV)])

s.task_start()
vbox

# %%
# s.task_stop()
