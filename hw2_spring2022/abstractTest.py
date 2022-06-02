import unittest
import hw2_spring2022.Solution as Solution


class AbstractTest(unittest.TestCase):
    # before each test, setUp is executed
    def setUp(self) -> None:
        Solution.dropTables()
        Solution.createTables()

    # after each test, tearDown is executed
    def tearDown(self) -> None:
        Solution.dropTables()
