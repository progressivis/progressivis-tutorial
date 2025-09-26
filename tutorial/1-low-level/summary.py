from progressivis import Max, Print, RandomPTable
from progressivis.core import aio


def _terse(_):
    print(".", end="", flush=True)


random = RandomPTable(10, rows=10_000)
# produces 10 columns named _1, _2, ...
max_ = Max()
max_.input[0] = random.output.result["_1", "_2", "_3"]
# Slot hints to restrict columns to ("_1", "_2", "_3")
pr = Print(proc=_terse)
pr.input[0] = max_.output.result

aio.run(random.scheduler.start())
