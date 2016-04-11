import falcon
import os
import sys
import time
import logging

from caas.router.caffe_model_resource import CaffeModelResource
from commons.src.load_balancer.worker_load_balancer import WorkerLoadBalancer
from model_workers_resource import ModelWorkersResource
from knnaas.router.predict import Predict
from knnaas.router.record import Record
from knnaas.router.knn_worker_client import KnnWorkerClient
from commons.src.logging.setup_logging import setup_logging
from commons.src.elb.elb_resource import ElbResource

def loader():
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting Router...")

    start_time = int(time.time())
    load_balancer = WorkerLoadBalancer()

    caffe_model_resource = CaffeModelResource(load_balancer)

    model_workers_resource = ModelWorkersResource(load_balancer)

    knn_worker_client = KnnWorkerClient(load_balancer)
    record_resource = Record(knn_worker_client)
    predict_resource = Predict(knn_worker_client)

    elb_resource = ElbResource(start_time, load_balancer)

    # falcon.API instances are callable WSGI apps
    app = falcon.API()

    app.add_route('/v1/caffe_model/predict', caffe_model_resource)

    app.add_route('/v1/model_workers_map', model_workers_resource)

    app.add_route('/v1/knn_model/update', record_resource)
    app.add_route('/v1/knn_model/predict', predict_resource)

    app.add_route('/elb-healthcheck', elb_resource)

    return app

