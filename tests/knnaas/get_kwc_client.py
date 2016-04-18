from commons.src.load_balancer.worker_load_balancer import WorkerLoadBalancer
from knnaas.router.knn_worker_client import KnnWorkerClient

def _get_kwc():
        routing_map = {'KNN': {'model-1':[{'host': 'localhost', 'port': 9090, 'local_worker_id': 0}, {'host': 'localhost', 'port': 9091, 'local_worker_id': 1}, {'host': 'localhost', 'port': 9092, 'local_worker_id': 2}, {'host': 'localhost', 'port': 9093, 'local_worker_id': 3}, {'host': 'localhost', 'port': 9094, 'local_worker_id': 4}, {'host': 'localhost', 'port': 9095, 'local_worker_id': 5}]}}
        wlb = WorkerLoadBalancer()
        wlb.update_workers_list(routing_map)
        kwc = KnnWorkerClient(wlb)
        return kwc
