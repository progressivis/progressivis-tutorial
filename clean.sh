#!/bin/sh
# Removes the automatically generated ipynb files.

find tutorial -name '.*' -type d | xargs rm -rf
find tutorial/1-low-level tutorial/4-visualization-creation -name '*.ipynb' | xargs rm -f
