
#
# Copyright 2012-2016, the original author or authors.

# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance
# with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "./build"))
import KNNINDEX
import logging
from commons.src.logging.setup_logging import setup_logging
logger = logging.getLogger(__name__)
SEPARATOR = ':'


def insert(data_point_id, vector, tags, knn_index):
    logger.info('[UPDATE] [INSERT] [STARTED]')
    tag_vector = KNNINDEX.LongVector()
    logger.debug('[Update] [insert] Update received long_dpid %s vector %s long_tags %s knn_index %s' % (data_point_id, vector, tags, knn_index))
    feature_vector = KNNINDEX.FloatVector()
    tag_vector.extend(tags)
    feature_vector.extend(vector)
    record = KNNINDEX.Record()
    record.set_key(data_point_id)
    record.set_feature_vector(feature_vector)
    record.set_tag_vector(tag_vector)
    logger.debug('[Update] [insert] record in update insert %s' % record)
    try:
        response = knn_index.insert(record)
        logger.debug('[Update] [insert] result response %s' % response)
        logger.info('[UPDATE] [INSERT] [DONE]')
        return response, 'OK'
    except RuntimeError as e:
        logger.error('[UPDATE] [INSERT] [CPP_RUNTIME_ERROR] %s' % e.message,exc_info=True)
        logger.info('[UPDATE] [INSERT] [DONE]')
        return False, e.message



def delete(ldpid, knn_index):
    logger.info('[UPDATE] [DELETE] [STARTED]')
    record = KNNINDEX.Record()
    record.set_key(ldpid)
    logger.debug('[Update] [delete] ldpid in remove %s' % ldpid)
    try:
        response= knn_index.remove(record)
        logger.debug('[Update] [delete] deletion response %s' % response)
        logger.info('[UPDATE] [DELETE] [DONE]')
        return response, 'OK'
    except RuntimeError as e:
        logger.error('[Update] [delete] Cpp_Runtime exception thrown %s' % e.message, exc_info=True)
        logger.info('[UPDATE] [DELETE] [DONE]')
        return False, e.message
