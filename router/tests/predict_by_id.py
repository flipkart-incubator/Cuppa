import requests
from commons.src.config import config_loader
worker_misc_config_path = "knnaas/config/worker_misc_config.yaml"
misc_config = config_loader.get(worker_misc_config_path)
import json
for i in range(9000):
    d = {"modelId":"model-1", "operation": "SearchById", "dataPointId": str(i), "tags": ["fdp-ml-test-tags"]}
    r = requests.post('http://localhost:8000/v1/knn_model/predict', data = json.dumps(d))
    assert(r.status == 200)
    assert(len(r.json()['result']) == misc_config['k'])
