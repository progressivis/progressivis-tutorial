"""
Module computing progressively the max of all the columns of a table.

This implementation adds internal decorators to reduce the code size.
It does support slot hints and quality.
"""
from typing import Any, Dict

import numpy as np
from progressivis import (Module, PDict, PTable, ReturnRunStep, def_input,
                          def_output)
from progressivis.core.decorators import process_slot, run_if_any
from progressivis.core.utils import fix_loc, indices_len


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
        self.quality: Dict[str, float] = {}
        self.default_step_size = 10000

    @process_slot("table", reset_cb="reset")  # v3
    @run_if_any  # v3
    def run_step(
        self, run_number: int, step_size: int, quantum: float
    ) -> ReturnRunStep:
        assert self.context  # v3
        with self.context as ctx:  # v3
            indices = ctx.table.created.next(length=step_size)  # v3
            steps = indices_len(indices)  # v3
            chunk = self.filter_slot_columns(ctx.table, fix_loc(indices))  # v3
            op = chunk.max(keepdims=False)  # v3
            if self.result is None:
                self.result = PDict(op)
            else:
                for k, v in self.result.items():
                    self.result[k] = _max_func(op[k], v)
            return self._return_run_step(self.next_state(ctx.table), steps)

    def reset(self) -> None:
        if self.result is not None:
            self.result.fill(-np.inf)

    def get_quality(self) -> Dict[str, float] | None:  # v3
        if self.result is None:
            return None
        for key in self.result:
            try:
                # The quality is simply the value.
                # It can only grow when improving, and
                # should stabilize eventually.
                self.quality["max_" + key] = float(self.result[key])
            except ValueError:
                pass
        return self.quality


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
