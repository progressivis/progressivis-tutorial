"""Module computing progressively the max of all the columns of a table.

This implementation is simple and does not use internal decorators.
It does support slot hints but not quality.
"""
from typing import Any

import numpy as np
from progressivis import (
    Module, ReturnRunStep, PTable, PDict,
    def_input, def_output
)
from progressivis.core.utils import indices_len, fix_loc


def _max_func(x: Any, y: Any) -> Any:  # v2
    try:  # fixing funny behaviour when max() is called with np.float64
        return np.fmax(x, y)  # np.fmax avoids propagation of Nan
    except Exception:
        return max(x, y)


@def_input("table", PTable, doc="The input PTable to process")
@def_output("result", PDict, doc=("PDict with max value of each column"))
class SimpleMax(Module):
    def __init__(self, **kwds: Any) -> None:
        super().__init__(**kwds)
        self.default_step_size = 10000

    def reset(self) -> None:
        if self.result is not None:
            self.result.fill(-np.inf)

    def run_step(
        self, run_number: int, step_size: int, quantum: float
    ) -> ReturnRunStep:
        # Handle the changed input slots
        table_slot = self.get_input_slot("table")
        if table_slot.updated.any() or table_slot.deleted.any():
            table_slot.reset()
            table_slot.update(run_number)
            self.reset()
        # Extract the new chunk
        indices = table_slot.created.next(length=step_size)
        steps = indices_len(indices)
        # The following helper function implements the slot hints
        chunk = self.filter_slot_columns(table_slot, fix_loc(indices))  # v2
        op = chunk.max(keepdims=False)  # v2
        # Update the result
        if self.result is None:
            self.result = PDict(op)
        else:
            for k, v in self.result.items():
                self.result[k] = _max_func(op[k], v)  # v2
        # Return the next state using a helper method and number of steps
        return self._return_run_step(self.next_state(table_slot), steps)  # v2


def _test_max():
    from progressivis import Print, RandomPTable, Scheduler
    from progressivis.core import aio
    s = Scheduler()
    random = RandomPTable(3, rows=10000, scheduler=s)
    max_ = SimpleMax(name="max_" + str(hash(random)), scheduler=s)
    max_.input[0] = random.output.result
    pr = Print(proc=_terse, scheduler=s)
    pr.input[0] = max_.output.result
    aio.run(s.start())
    assert random.result is not None
    assert max_.result is not None
    res1 = random.result.max()
    res2 = max_.result
    _compare(res1, res2)


def _test_max_cols():
    from progressivis import Print, RandomPTable, Scheduler
    from progressivis.core import aio
    s = Scheduler()
    random = RandomPTable(10, rows=10000, scheduler=s)
    max_ = SimpleMax(name="max_" + str(hash(random)), scheduler=s)
    max_.input[0] = random.output.result["_1", "_2", "_3"]
    pr = Print(proc=_terse, scheduler=s)
    pr.input[0] = max_.output.result
    aio.run(s.start())
    assert random.result is not None
    assert max_.result is not None
    res1 = random.result.loc[:, ["_1", "_2", "_3"]].max()
    res2 = max_.result
    _compare(res1, res2)


def _compare(res1, res2):
    import numpy as np
    v1 = np.array(list(res1.values()))
    v2 = np.array(list(res2.values()))
    assert np.allclose(v1, v2)


def _terse(_):
    print(".", end="", flush=True)


if __name__ == "__main__":
    _test_max()
    _test_max_cols()
