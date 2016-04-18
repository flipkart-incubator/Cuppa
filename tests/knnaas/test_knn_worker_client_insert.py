from knnaas.router.knn_worker_client import KnnWorkerClient
from commons.src.load_balancer.worker_load_balancer import WorkerLoadBalancer
import unittest
import get_kwc_client
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
        kwc = get_kwc_client._get_kwc()
        for i in range(15001, 25000):
            if i % 2 == 0:
                by = 1
                dpid = str(i)
                responses, message= kwc._insert('model-1', str(i), [i, 2*i, 3*i, 4*i], ['fdp-ml-test-tag-4'])
                assert(responses)
                for uo in responses:
                    print uo.status
                    assert(uo.status == True)
                    assert(uo.message == 'OK')
                assert(message == 'OK')
                # po = self._predict(model_id, None, tags, dpid, by)
                # assert(len(po.values) == misc_config['k'])
                # assert(po.message == 'OK')
            else:
                responses, message = kwc._insert('model-1', str(i), [i, 2*i, 3*i, 4*i, 5*i], ['fdp-ml-test-tag-5'])
                assert(responses)
                for uo in responses:
                    assert(uo.status == True)
                    assert(uo.message == 'OK')
                assert(message == 'OK')


if __name__ == "__main__":
    unittest.main()
