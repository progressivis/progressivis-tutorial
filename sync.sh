#!/bin/sh
# Creates the ipynb files and synchronize them all.

find tutorial/1-low-level tutorial/4-visualization-creation -name '*.py'  -not -path "*/.ipynb_checkpoints/*" | xargs -I{} jupytext --set-formats ipynb,py {}
find tutorial/1-low-level tutorial/4-visualization-creation -name '*.ipynb' -not -path "*/.ipynb_checkpoints/*" | xargs -I{} jupytext --sync {}
