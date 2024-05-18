from collections import OrderedDict
from threading import Lock

class MathCache:
    def __init__(self):
        self.store = {}
        self.cache = OrderedDict()
        self.cache_size = 10
        self.lock = Lock()

    def Set(self, key, value):
        with self.lock:
            self.store[key] = value
            self.cache.clear()

    def Get(self, key):
        return self.store[key]

    def _add(self, a, b): return a + b
    def _sub(self, a, b): return a - b
    def _mult(self, a, b): return a * b
    def _div(self, a, b): return a / b

    def _cache_op(self, operation, key_a, key_b):
        with self.lock:
            cache_key = (operation, key_a, key_b)
            if cache_key in self.cache:
                self.cache.move_to_end(cache_key)
                return self.cache[cache_key], True

        result = getattr(self, f"_{operation}")(self.Get(key_a), self.Get(key_b))

        with self.lock:
            if len(self.cache) >= self.cache_size:
                self.cache.popitem(last=False)
            self.cache[cache_key] = result
            return result, False

    def Add(self, key_a, key_b): return self._cache_op("add", key_a, key_b)
    def Sub(self, key_a, key_b): return self._cache_op("sub", key_a, key_b)
    def Mult(self, key_a, key_b): return self._cache_op("mult", key_a, key_b)
    def Div(self, key_a, key_b): return self._cache_op("div", key_a, key_b)

import grpc
from concurrent import futures
import traceback
import mathdb_pb2
import mathdb_pb2_grpc

class MathDb(mathdb_pb2_grpc.MathDbServicer):
    def __init__(self):
        self.cache = MathCache()

    def Set(self, request, context):
        try:
            self.cache.Set(request.key, request.value)
            return mathdb_pb2.SetResponse(error="")
        except Exception as e:
            return mathdb_pb2.SetResponse(error=traceback.format_exc())

    def Get(self, request, context):
        try:
            value = self.cache.Get(request.key)
            return mathdb_pb2.GetResponse(value=value, error="")
        except Exception as e:
            return mathdb_pb2.GetResponse(value=0.0, error=traceback.format_exc())

    def Add(self, request, context):
        try:
            value, cache_hit = self.cache.Add(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value=value, cache_hit=cache_hit, error="")
        except Exception as e:
            return mathdb_pb2.BinaryOpResponse(value=0.0, cache_hit=False, error=traceback.format_exc())

    def Sub(self, request, context):
        try:
            value, cache_hit = self.cache.Sub(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value=value, cache_hit=cache_hit, error="")
        except Exception as e:
            return mathdb_pb2.BinaryOpResponse(value=0.0, cache_hit=False, error=traceback.format_exc())

    def Mult(self, request, context):
        try:
            value, cache_hit = self.cache.Mult(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value=value, cache_hit=cache_hit, error="")
        except Exception as e:
            return mathdb_pb2.BinaryOpResponse(value=0.0, cache_hit=False, error=traceback.format_exc())

    def Div(self, request, context):
        try:
            value, cache_hit = self.cache.Div(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value=value, cache_hit=cache_hit, error="")
        except Exception as e:
            return mathdb_pb2.BinaryOpResponse(value=0.0, cache_hit=False, error=traceback.format_exc())


if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
    mathdb_pb2_grpc.add_MathDbServicer_to_server(MathDb(), server)
    server.add_insecure_port("[::]:5440")
    print('works')
    server.start()
    print('works')
    server.wait_for_termination()