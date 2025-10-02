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
from progressivis import Max, Print, RandomPTable


# %%
def _terse(_):
    print(".", end="", flush=True)


# %%
random = RandomPTable(10, rows=10_000)
# produces 10 columns named _1, _2, ...
max_ = Max()
max_.input[0] = random.output.result["_1", "_2", "_3"]
# Slot hints to restrict columns to ("_1", "_2", "_3")
pr = Print(proc=_terse)
pr.input[0] = max_.output.result

# %%
try:
    import graphviz
    src = random.scheduler.to_graphviz()
    gvz = graphviz.Source(src)
    display(gvz)
except ImportError as e:
    print(e)
    pass

# %%

# %%
from progressivis.core import aio

try:  # Run in plain Python
    aio.run(random.scheduler.start())
except RuntimeError:  # Run in the notebook
    random.scheduler.task_start()

# %%
