import unittest 
from cuchillo_de_gaucho.utilities import add, multiply
 
class TestUtilities(unittest.TestCase): 
    def test_add(self): 
        self.assertEqual(add(1, 2), 3) 
 
    def test_multiply(self): 
        self.assertEqual(multiply(2, 3), 6) 
 
if __name__ == "__main__": 
    unittest.main() 
