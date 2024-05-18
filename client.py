import grpc
import sys
import threading
import csv
from concurrent import futures
import mathdb_pb2
import mathdb_pb2_grpc

global_hits = 0
global_misses = 0
lock = threading.Lock()

def process_csv(filename, channel):
    global global_hits, global_misses
    stub = mathdb_pb2_grpc.MathDbStub(channel)
    
    with open(filename, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            operation = row['operation']
            keys = [row.get('key_a'), row.get('key_b')]
            if operation in ['set', 'get', 'add', 'sub', 'mult', 'div']:
                func = getattr(stub, operation.capitalize())
                if operation == 'set':
                    response = func(mathdb_pb2.SetRequest(key=row['key_a'], value=float(row['key_b'])))
                else:
                    response = func(mathdb_pb2.BinaryOpRequest(key_a=row['key_a'], key_b=row['key_b']))
                if operation in ['add', 'sub', 'mult', 'div']:
                    with lock:
                        if response.cache_hit:
                            global_hits += 1
                        else:
                            global_misses += 1

def main(port, csv_files):
    channel = grpc.insecure_channel(f'localhost:{port}')
    threads = []

    for filename in csv_files:
        thread = threading.Thread(target=process_csv, args=(filename, channel,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    total = global_hits + global_misses
    hit_rate = global_hits / total if total>0 else 0
    print(hit_rate)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        sys.exit(1)
    main(sys.argv[1], sys.argv[2:])