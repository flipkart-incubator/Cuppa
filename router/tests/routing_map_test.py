import requests
import json
import falcon

#routing_map = {'KNN': {'model-1':[{'host': 'localhost', 'port': 9090, 'local_worker_id': 0}, {'host': 'localhost', 'port': 9091, 'local_worker_id': 1}, {'host': 'localhost', 'port': 9092, 'local_worker_id': 2}, {'host': 'localhost', 'port': 9093, 'local_worker_id': 3}, {'host': 'localhost', 'port': 9094, 'local_worker_id': 4}, {'host': 'localhost', 'port': 9095, 'local_worker_id': 5}]}}

routing_map = {'KNN': {'model-1':[{'host': 'localhost', 'port': 9090, 'local_worker_id': 0}, {'host': 'localhost', 'port': 9091, 'local_worker_id': 1}, {'host': 'localhost', 'port': 9092, 'local_worker_id': 2}]}}

r = requests.put('http://localhost:8000/v1/model_workers_map', data = json.dumps(routing_map))
assert(r.status_code == 200)
