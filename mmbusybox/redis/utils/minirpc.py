from __future__ import annotations

import pickle
from types import TracebackType
from typing import ContextManager, Generator, ItemsView, KeysView, Type, ValuesView

import redis

def unpickle(get_result):
    return pickle.loads(bytes.fromhex(get_result))


def stream_reader(conn):
    i = 0
    while True:
        try:
            [(r, g)] = conn.xrange("tests:stream", f'1671665641501-{i}', "+", 1)
            yield r, g

            i += 1
        except ValueError as err:
            break


class RStreamReader(ContextManager):
    def __init__(self, conn, stream_name="tests:stream", stream_id="*", start=0, end: str | int = "+", count=1):
        self.conn = conn

        self.stream_name, self.stream_id, self.start, self.end, self.count = stream_name, stream_id, start, end, count
        self.i = self.start

    def __enter__(self):

        while True:

            try:
                [(r, g)] = self.conn.xrange(self.stream_name, f'{self.stream_id}-{self.i}', self.end, self.count)
                yield r, g
                self.i += 1
            except ValueError as err:
                pass
            except KeyboardInterrupt as err:
                yield self.stream_id, self.i
                break

    def __exit__(self, __exc_type: Type[BaseException] | None, __exc_value: BaseException | None,
                 __traceback: TracebackType | None) -> bool | None:
        self.i = 0


def stream(conn: redis.Redis, name: str, stream_id: str = f'1671665641501', start: int | str = 0, end: str | int = "+",
           count: int = 1) -> Generator[..., str]:
    with RStreamReader(conn, name, stream_id, start, end, count) as gen:
        yield from gen


class RC(dict):
    def __init__(self, mk="ug:test:", conn=None):
        dict.__init__(self)
        self._keys = []

        self.root_key = mk
        self.conn = conn

    def __getitem__(self, pk):

        return pickle.loads(bytes.fromhex(self.conn.get(self.root_key + pk)))

    def __setitem__(self, pk, item):
        if pk not in self._keys:
            self._keys.append(pk)
        self.conn.set(self.root_key + pk, pickle.dumps(item).hex())

    def keys(self) -> KeysView:

        return KeysView(self._keys)

    def items(self) -> ItemsView:
        self._items = []
        for k in self._keys:
            self._items.append((k, self[k]))
        return ItemsView(self._items)

    def values(self) -> KeysView:
        def generate():
            for k in self._keys:
                yield self[k]

        return ValuesView(list(generate()))


def simple_rpc(redis_stream: Generator[..., str]):
    for g, v in redis_stream:
        if "command" in v.keys():
            print(v)

            try:
                res = eval(v["command"])
                print(f"eval: {v['command']} = {res}")
            except SyntaxError as err:

                res = exec(v["command"])
                print(f"exec: {v['command']} = {res}")
            except Exception as err:
                print(err)
                continue
        else:

            print(f"pass")