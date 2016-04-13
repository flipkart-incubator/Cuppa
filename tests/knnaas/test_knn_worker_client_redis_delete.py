from knnaas.router.knn_worker_client import KnnWorkerClient
from commons.src.load_balancer.worker_load_balancer import WorkerLoadBalancer
import unittest

class SimpleInsertTest(unittest.TestCase):


    def _get_kwc(self):
        routing_map = {'KNN': {'model-1':[{'host': 'localhost', 'port': 9090, 'local_worker_id': 0}]}}
        wlb = WorkerLoadBalancer()
        wlb.update_workers_list(routing_map)
        kwc = KnnWorkerClient(wlb)
        return kwc

    def test_redis_remove(self):
        # embd = [2.11, 3.122, 4.212, 2.113]
        # tags = ["fdp-ml-test-tags"]
        kwc = self._get_kwc()
        for i in range(9000):
            if i % 2 == 0:
                by = 1
                dpid = str(i)
                uo, message= kwc._redis_delete('model-1', str(i))
                print uo.message
                assert(uo)
                assert(uo.status == True)
                assert(uo.message == 'OK')
                assert(message == 'OK')
                # po = self._predict(model_id, None, tags, dpid, by)
                # assert(len(po.values) == misc_config['k'])
                # assert(po.message == 'OK')
            else:
                uo, message = kwc._redis_delete('model-1', str(i))
                assert(uo)
                assert(uo.status == True)
                assert(uo.message == 'OK')
                assert(message == 'OK')


if __name__ == "__main__":
    unittest.main()
