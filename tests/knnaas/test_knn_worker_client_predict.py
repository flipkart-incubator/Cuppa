from knnaas.router.knn_worker_client import KnnWorkerClient
from commons.src.config import config_loader
worker_misc_config_path = "conf/knnaas_config.yaml"
misc_config = config_loader.get(worker_misc_config_path)
from commons.src.load_balancer.worker_load_balancer import WorkerLoadBalancer
import unittest

class SimpleInsertTest(unittest.TestCase):


    def _get_kwc(self):
        routing_map = {'KNN': {'model-1':[{'host': 'localhost', 'port': 9090, 'local_worker_id': 0}]}}
        wlb = WorkerLoadBalancer()
        wlb.update_workers_list(routing_map)
        kwc = KnnWorkerClient(wlb)
        return kwc

    def test_insert(self):
        # embd = [2.11, 3.122, 4.212, 2.113]
        # tags = ["fdp-ml-test-tags"]
        kwc = self._get_kwc()
        for i in range(90):
            if i % 2 == 0:
                by = 1
                dpid = str(i)
                klr, message = kwc._predict('model-1', [i, 2*i, 3*i, 4*i], ['fdp-ml-test-tag-4'], str(i), by)
                assert(klr)
                assert(len(klr.values) ==  misc_config['k'])
                assert(klr.message == 'OK')
                assert(message == 'OK')
                # po = self._predict(model_id, None, tags, dpid, by)
                # assert(len(po.values) == misc_config['k'])
                # assert(po.message == 'OK')
            else:
                by = 0
                klr, message = kwc._predict('model-1', [i, 2*i, 3*i, 4*i, 5*i], ['fdp-ml-test-tag-5'], None, by)
                assert(klr)
                assert(len(klr.values)  == misc_config['k'])
                assert(klr.message == 'OK')
                assert(message == 'OK')


if __name__ == "__main__":
    unittest.main()
