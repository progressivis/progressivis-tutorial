from __future__ import annotations

import logging
import pandas as pd
from progressivis import ProgressiveError
from progressivis.core.docstrings import RESULT_DOC
from progressivis.utils.inspect import filter_kwds
from progressivis.core.module import Module
from progressivis.core.module import ReturnRunStep, def_output
from progressivis.core.utils import force_valid_id_columns
from progressivis.table.table import PTable
from progressivis.table.dshape import dshape_from_dataframe

from typing import (Dict, Any, Tuple)

logger = logging.getLogger(__name__)


@def_output("result", PTable, doc=RESULT_DOC)
class SmallCSVLoaderV1(Module):
    def __init__(self, filepath_or_buffer: Any, **kwds: Any) -> None:
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
        self.parser = pd.read_csv(filepath_or_buffer, **csv_kwds)
        self._rows_read = 0
        self.result: PTable | None  # to help mypy

    def rows_read(self) -> int:
        return self._rows_read

    def is_data_input(self) -> bool:
        return True

    def run_step(
        self, run_number: int, step_size: int, quantum: float
    ) -> ReturnRunStep:
        if step_size == 0:  # bug
            return self._return_run_step(self.state_ready, steps_run=0)
        try:
            df = self.parser.read(step_size)
        except StopIteration:
            return self._return_run_step(self.state_zombie, steps_run=0)
        except ValueError:
            raise
        creates = len(df)
        if creates == 0:  # should not happen
            logger.error("Received 0 elements")
            return self._return_run_step(self.state_zombie, steps_run=0)
        self._rows_read += creates
        force_valid_id_columns(df)  # fix column names
        if self.result is None:  # create the PTable
            self.result = PTable(
                name=self.generate_table_name("table"),
                dshape=dshape_from_dataframe(df),  # infer types
                data=df,
                create=True
            )
        else:
            self.result.append(df)
        return self._return_run_step(self.state_ready, steps_run=creates)

    def get_progress_FAKE(self) -> Tuple[int, int]:
        input_size = self.parser._input._input_size
        if input_size == 0:
            return (0, 0)
        pos = self.parser._input._stream.tell()
        length = len(self.result)
        estimated_row_size = pos / length
        estimated_size = int(input_size / estimated_row_size)
        return (length, estimated_size)


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
