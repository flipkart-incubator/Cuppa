import requests
import json

d = {"model-1": [{"host":"localhost","port":9090, "local_worker_id" : 0}]}

r = requests.put('http://localhost:8000/v1/model_workers_map', data = json.dumps(d))
assert(r.status == 200)
