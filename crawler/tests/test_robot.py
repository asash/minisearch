import unittest
import sys
sys.path.append('..')

from init_robots import init_robots

class TestRobots(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_robots(self):
        robot_filter = init_robots({'habrahabr.ru', 'geektimes.ru'})
        self.assertFalse(robot_filter('http://habrahabr.ru'))
        self.assertTrue(robot_filter('http://habrahabr.ru/search/dadadf'))


if __name__ == '__main__':
    unittest.main()

