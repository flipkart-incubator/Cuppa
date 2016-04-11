import os
import sys
import time
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../"))

from commons.src.load_balancer.worker_load_balancer import WorkerLoadBalancer
from commons.src.load_balancer.worker_info import WorkerInfo

class WorkerLoadBalancerTestCase(unittest.TestCase):
    """
    Tests for worker_load_balancer.py
    """

    def setUp(self):
        """
        This method is called before each test
        """
        self.worker_load_balancer = WorkerLoadBalancer()

    def tearDown(self):
        """
        This method is called after each test
        """
        pass

    def test_update_workers_list(self):
        pass

    def test_get_all_workers(self):
        self.assertEqual(self.worker_load_balancer.get_all_workers("Caffe"), None)
        self.assertEqual(self.worker_load_balancer.get_all_workers("KNN"), None)

    def test_get_model_to_workers_list(self):
        self.assertEqual(self.worker_load_balancer.get_model_to_workers_list("Caffe"), None)
        self.assertEqual(self.worker_load_balancer.get_model_to_workers_list("KNN"), None)

    def test_get_worker_info(self):
        self.assertEqual(self.worker_load_balancer.get_worker_info("worker-1", "Caffe"), None)

    def test_check_if_model_to_workers_map_is_empty(self):
        self.assertEqual(self.worker_load_balancer.check_if_model_to_workers_map_is_empty(), True)
        model_type_to_model_to_workers_map = {}
        caffe_model_to_workers_map = {}
        knn_model_to_workers_map = {}
        caffe_worker_01 = {}
        caffe_worker_01["host"] = 'localhost'
        caffe_worker_01["port"] = 9090
        caffe_worker_01["local_worker_id"] = 0

        caffe_worker_02 = {}
        caffe_worker_02["host"] = 'localhost'
        caffe_worker_02["port"] = 9091
        caffe_worker_02["local_worker_id"] = 1

        caffe_workers = []
        caffe_workers.append(caffe_worker_01)
        caffe_workers.append(caffe_worker_02)


        knn_worker_01 = {}
        knn_worker_01["host"] = 'localhost'
        knn_worker_01["port"] = 9090
        knn_worker_01["local_worker_id"] = 0

        knn_worker_02 = {}
        knn_worker_02["host"] = 'localhost'
        knn_worker_02["port"] = 9091
        knn_worker_02["local_worker_id"] = 1

        knn_workers = []
        knn_workers.append(knn_worker_01)
        knn_workers.append(knn_worker_02)

        caffe_model_to_workers_map["model-1"] = caffe_workers
        knn_model_to_workers_map["model-2"] = knn_workers

        model_type_to_model_to_workers_map["Caffe"] = caffe_model_to_workers_map
        model_type_to_model_to_workers_map["KNN"] = knn_model_to_workers_map

        self.worker_load_balancer.update_workers_list(model_type_to_model_to_workers_map)

        self.assertEqual(self.worker_load_balancer.check_if_model_to_workers_map_is_empty(), False)

        caffe_workers_to_be_deleted = [WorkerInfo('localhost', 9090, 0), WorkerInfo('localhost', 9091, 1)]

        self.worker_load_balancer.remove_workers("Caffe", "model-1", caffe_workers_to_be_deleted)


    def test_choose_worker(self):
        self.assertRaises(Exception, lambda: self.worker_load_balancer.choose_worker("Caffe", "model-1"))
        model_type_to_model_to_workers_map = {}
        caffe_model_to_workers_map = {}
        knn_model_to_workers_map = {}
        caffe_worker_01 = {}
        caffe_worker_01["host"] = 'localhost'
        caffe_worker_01["port"] = 9090
        caffe_worker_01["local_worker_id"] = 0

        caffe_worker_02 = {}
        caffe_worker_02["host"] = 'localhost'
        caffe_worker_02["port"] = 9091
        caffe_worker_02["local_worker_id"] = 1

        caffe_workers = []
        caffe_workers.append(caffe_worker_01)
        caffe_workers.append(caffe_worker_02)


        knn_worker_01 = {}
        knn_worker_01["host"] = 'localhost'
        knn_worker_01["port"] = 9090
        knn_worker_01["local_worker_id"] = 0

        knn_worker_02 = {}
        knn_worker_02["host"] = 'localhost'
        knn_worker_02["port"] = 9091
        knn_worker_02["local_worker_id"] = 1

        knn_workers = []
        knn_workers.append(knn_worker_01)
        knn_workers.append(knn_worker_02)

        caffe_model_to_workers_map["model-1"] = caffe_workers
        knn_model_to_workers_map["model-2"] = knn_workers

        model_type_to_model_to_workers_map["Caffe"] = caffe_model_to_workers_map
        model_type_to_model_to_workers_map["KNN"] = knn_model_to_workers_map

        self.worker_load_balancer.update_workers_list(model_type_to_model_to_workers_map)

        self.assertRaises(Exception, lambda: self.worker_load_balancer.choose_worker("Caffe", "model-2"))

        worker = self.worker_load_balancer.choose_worker("Caffe", "model-1")

        self.assertEqual(worker.host, 'localhost')
        self.assertTrue(worker.port in [9090, 9091])
        self.assertTrue(worker.local_worker_id in [0, 1])

    def test_remove_workers(self):
        model_type_to_model_to_workers_map = {}
        caffe_model_to_workers_map = {}
        knn_model_to_workers_map = {}
        caffe_worker_01 = {}
        caffe_worker_01["host"] = 'localhost'
        caffe_worker_01["port"] = 9090
        caffe_worker_01["local_worker_id"] = 0

        caffe_worker_02 = {}
        caffe_worker_02["host"] = 'localhost'
        caffe_worker_02["port"] = 9091
        caffe_worker_02["local_worker_id"] = 1

        caffe_workers = []
        caffe_workers.append(caffe_worker_01)
        caffe_workers.append(caffe_worker_02)


        knn_worker_01 = {}
        knn_worker_01["host"] = 'localhost'
        knn_worker_01["port"] = 9090
        knn_worker_01["local_worker_id"] = 0

        knn_worker_02 = {}
        knn_worker_02["host"] = 'localhost'
        knn_worker_02["port"] = 9091
        knn_worker_02["local_worker_id"] = 1

        knn_workers = []
        knn_workers.append(knn_worker_01)
        knn_workers.append(knn_worker_02)

        caffe_model_to_workers_map["model-1"] = caffe_workers
        knn_model_to_workers_map["model-2"] = knn_workers

        model_type_to_model_to_workers_map["Caffe"] = caffe_model_to_workers_map
        model_type_to_model_to_workers_map["KNN"] = knn_model_to_workers_map

        self.worker_load_balancer.update_workers_list(model_type_to_model_to_workers_map)

        caffe_workers_to_be_removed = [WorkerInfo('localhost', 9090, 0), WorkerInfo('localhost', 9091, 1)]

        self.worker_load_balancer.remove_workers('Caffe', 'model-1', caffe_workers_to_be_removed)

        self.assertRaises(Exception, lambda : self.worker_load_balancer.choose_worker('Caffe', 'model-1'))

        pass

if __name__ == '__main__':
    unittest.main()