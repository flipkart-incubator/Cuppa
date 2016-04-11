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

import yaml

def get(config_path):
    """
    To read YAML file as a dict
    Args:
        config_path: YAML file path

    Returns: dict from yaml

    """
    try:
        with open(config_path) as config_file:
            config = yaml.load(config_file)
            return config
    except IOError as error:
        raise error

