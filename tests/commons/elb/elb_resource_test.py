import os
import sys
import time
import json
import unittest
import falcon
from mock import Mock
from mock import MagicMock

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../"))


from commons.src.elb.elb_resource import ElbResource
from commons.src.load_balancer.worker_load_balancer import WorkerLoadBalancer
from commons.src.load_balancer.worker_info import WorkerInfo

class ElbResourceTest(unittest.TestCase):
	def setUp(self):
		self.load_balancer = WorkerLoadBalancer()
		self.load_balancer.check_if_model_to_workers_map_is_empty = MagicMock(return_value = False)
		start_time = int(time.time())
		self.elb_resource = ElbResource(start_time, self.load_balancer)

	def tearDown(self):
		pass

	def test_on_get(self):
		time.sleep(1)
		req = None
		resp = falcon.Response()
		resp.status = None
		resp.body = None
		self.elb_resource.on_get(req, resp)
		response_body = json.loads(resp.body)
		self.assertEquals(response_body['capacity'], 100)
		self.assertEquals(response_body['requests'], 1)
		self.assertGreaterEqual(response_body['uptime'], 1)
		self.assertEquals(resp.status, falcon.HTTP_200)

		self.load_balancer.check_if_model_to_workers_map_is_empty = MagicMock(return_value = True)

		resp_1 = falcon.Response()
		resp_1.status = None
		resp_1.body = None

		self.assertRaises(falcon.HTTPInternalServerError, lambda : self.elb_resource.on_get(req, resp_1))


if __name__ == '__main__':
    unittest.main()
