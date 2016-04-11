import falcon
import json
import sys
import logging

class ModelWorkersResource(object):
    """
    To expose Restful endpoints related to Model to Workers mapping
    """
    def __init__(self, load_balancer):
        self.load_balancer = load_balancer
        self.logger = logging.getLogger(__name__)

    def on_put(self, req, resp):
        """
        Method to update model to workers map
        :param req:
        :param resp:
        :return:
        """
        model_to_workers_map = json.loads(req.stream.read())
        self.logger.debug('Update request received with payload: %s', str(model_to_workers_map))
        self.load_balancer.update_workers_list(model_to_workers_map)
        resp.status = falcon.HTTP_200

    def on_get(self, req, resp):
        """Handles GET requests"""
        model_type = req.get_param('model-type')
        resp.status = falcon.HTTP_200  # This is the default status
        if model_type:
            resp.body = str(self.load_balancer.get_model_to_workers_list(model_type))
        else:
            resp.status = falcon.HTTP_400
            raise falcon.HTTPBadRequest("Bad Request", "model-type is missing in query params")
