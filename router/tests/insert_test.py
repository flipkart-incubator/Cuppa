import requests
import json
for i in range(500):
    d = {"modelId":"model-1", "operation": str("insert"), "dataPointId": str(i), "vector": [float(i), float(2*i+1), float(2*i+2), float(2*i+3)], "tags": ["fdp-ml-test-tags"]}
    r = requests.post('http://localhost:8000/v1/knn_model/update', data = json.dumps(d))
    assert(r.status_code == 200)
