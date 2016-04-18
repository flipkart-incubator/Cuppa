import json
from falcon import status_codes
import logging
logger = logging.getLogger(__name__)


IV = 'vector'
TAGS = 'tags'
TIMESTAMP = 'timestamp'
INDEXED = 'indexed'
INSERTED = 'inserted'
EXISTS = 'exists'




def redis_insert_data_point(model_id, data_point_id, image_vector, tags, timestamp, redis_client):
    data = json.dumps({
            IV: image_vector,
            TAGS: tags,
            TIMESTAMP: timestamp
        })
    redis_store_resp, message = redis_client.store_model_data_point(model_id, data_point_id, data)
    return redis_store_resp, message

def redis_get_all_data_points(model_id, redis_client):
    logger.info("redis get all data started ...")
    dumped_data, message = redis_client.get_all_data(model_id)
    loaded_data = dict()
    if dumped_data == False:
        return False, message
    if len(dumped_data) == 0:
        return False, 'No Data in Redis against this model id'
    if dumped_data and len(dumped_data) > 0:
        for key, value in dumped_data.iteritems():
            if value and len(value) > 0:
                loaded_data[key] = json.loads(value)
    return loaded_data, 'OK'

def redis_delete_data_point(model_id, data_point_id, redis_client):
    response, message = redis_client.delete_data_point(model_id, data_point_id)
    return response, message
