from knnaas.router.knn_worker_client import KnnWorkerClient
from commons.src.config import config_loader
worker_misc_config_path = "conf/knnaas_config.yaml"
misc_config = config_loader.get(worker_misc_config_path)
from commons.src.load_balancer.worker_load_balancer import WorkerLoadBalancer
import unittest
