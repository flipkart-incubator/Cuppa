import os
import sys
import time
import unittest

sys.path.append(os.path.join(os.path.dirname(__file__), "../../../"))

from commons.src.config import config_loader

class ConfigLoaderTest(unittest.TestCase):
	def setUp(self):
		pass

	def tearDown(self):
		pass

	def test_get_config(self):
		self.assertEquals(len(config_loader.get('../../resources/test_config_1.yaml').keys()), 2)
		self.assertEquals(set(config_loader.get('../../resources/test_config_1.yaml').keys()), set(['key_1', 'key_2']))
		self.assertEquals(config_loader.get('../../resources/test_config_1.yaml').get('key_1'), 1)
		self.assertEquals(config_loader.get('../../resources/test_config_1.yaml').get('key_2'), 2)
		self.assertRaises(IOError, lambda : config_loader.get('../../resources/invalid_config_path.yaml'))
		self.assertRaises(TypeError, lambda : config_loader.get(None))
		
if __name__ == '__main__':
    unittest.main()