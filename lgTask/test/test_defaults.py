import unittest
import package_name

class TestDefaults(unittest.TestCase):
    """Test Default Parameters"""

    def test_one(self):
        exp = 'This is an example package!'
        if package_name.test() != exp:
            raise Exception('Expected ' + a)

