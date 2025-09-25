from __future__ import annotations

import logging
import pandas as pd
from progressivis import ProgressiveError
from progressivis.core.docstrings import RESULT_DOC
from progressivis.utils.inspect import filter_kwds
from progressivis.core.module import Module
from progressivis.core.module import ReturnRunStep, def_output, document
from progressivis.core.utils import force_valid_id_columns
from progressivis.table.table import PTable
from progressivis.table.dshape import dshape_from_dataframe

from typing import (
    Dict,
    Any,
)

logger = logging.getLogger(__name__)


@document
@def_output("result", PTable, doc=RESULT_DOC)
class SmallCSVLoaderV1(Module):
    """
    This module reads comma-separated values (csv) files progressively into a {{PTable}}.
    Internally it uses :func:`pandas.read_csv`.
    """

    def __init__(
            self,
            filepath_or_buffer: Any,
            fillvalues: dict[str, Any] | None = None,
            **kwds: Any,
    ) -> None:
        r"""
        Args:
            filepath_or_buffer: str, path object or file-like object accepted by :func:`pandas.read_csv`
            kwds: extra keyword args to be passed to :func:`pandas.read_csv` and :class:`Module <progressivis.core.Module>` superclass


        """
        if "index_col" in kwds:
            raise ProgressiveError("'index_col' parameter is not supported")
        super().__init__(**kwds)
        self.default_step_size = 1000
        chunksize_ = kwds.get("chunksize")
        if isinstance(chunksize_, int):  # initial guess
            self.default_step_size = chunksize_
        if chunksize_ is None:
            kwds["chunksize"] = self.default_step_size
        else:
            kwds.setdefault("chunksize", self.default_step_size)
        # Filter out the module keywords from the csv loader keywords
        csv_kwds: Dict[str, Any] = filter_kwds(kwds, pd.read_csv)
        csv_kwds["index_col"] = False
        self.filepath_or_buffer = filepath_or_buffer
        self.parser: pd.io.parsers.readers.TextFileReader | None = None
        self.csv_kwds = csv_kwds
        self._rows_read = 0
        self._fillvalues = fillvalues
        self.result: PTable | None  # to help mypy

    def rows_read(self) -> int:
        return self._rows_read

    def is_data_input(self) -> bool:
        # pylint: disable=no-self-use
        "Return True if this module brings new data"
        return True

    def run_step(
        self, run_number: int, step_size: int, quantum: float
    ) -> ReturnRunStep:
        if step_size == 0:  # bug
            logger.error("Received a step_size of 0")
            return self._return_run_step(self.state_ready, steps_run=0)
        if self.parser is None:
            try:
                self.parser = pd.read_csv(self.filepath_or_buffer,
                                          **self.csv_kwds)
            except IOError as e:
                logger.error("Cannot open file %s: %s",
                             self.filepath_or_buffer, e)
                self._return_run_step(self.state_zombie, steps_run=0)
        logger.info("loading %d lines", step_size)
        try:
            assert self.parser is not None
            df: pd.DataFrame = self.parser.read(step_size)
        except StopIteration:
            self.parser = None
            return self._return_run_step(self.state_zombie, steps_run=0)
        except ValueError:
            raise
        creates = len(df)
        if creates == 0:  # should not happen
            logger.error("Received 0 elements")
            return self._return_run_step(self.state_zombie, steps_run=0)
        self._rows_read += creates
        logger.info("Loaded %d lines", self._rows_read)
        force_valid_id_columns(df)
        dshape = dshape_from_dataframe(df)
        if self.result is None:
            table_params: Dict[str, Any] = dict(name=self.name,
                                                fillvalues=self._fillvalues)
            table_params["name"] = self.generate_table_name("table")
            table_params["dshape"] = dshape
            table_params["data"] = df
            table_params["create"] = True
            self.result = PTable(**table_params)
        else:
            self.result.append(df)
        return self._return_run_step(self.state_ready, steps_run=creates)


def _test_1():
    from progressivis.core import aio
    from progressivis import Scheduler, get_dataset, Sink
    s = Scheduler()
    module = SmallCSVLoaderV1(get_dataset("bigfile"), header=None, scheduler=s)
    sink = Sink(name="sink", scheduler=s)
    sink.input.inp = module.output.result
    aio.run(s.start())
    assert module.result is not None
    assert len(module.result) == 1_000_000


def _test_2():
    from progressivis.core import aio
    from progressivis import Scheduler, Sink
    from progressivis.core.utils import RandomBytesIO
    s = Scheduler()
    length = 30_000
    module = SmallCSVLoaderV1(
        RandomBytesIO(cols=30, rows=length),
        header=None,
        scheduler=s,
    )
    sink = Sink(name="sink", scheduler=s)
    sink.input.inp = module.output.result
    aio.run(s.start())
    assert module.result is not None
    assert len(module.result) == length


if __name__ == "__main__":
    _test_1()
    _test_2()
