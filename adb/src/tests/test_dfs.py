import unittest
from dfs import Graph


class TestGraph(unittest.TestCase):
    """ Test Cases for Graph """
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        # self.dag = Graph()
        # self.dag.insert_vertex(0)
        # for i in range(1, 10):
        #     self.dag.insert_vertex(i)
        #     self.dag.update_adjacent(i - 1, i)
        # self.dag.insert_vertex(10)
        # # self.dag.update_adjacent(10, 11)
        # self.cycle_g = Graph()
        # self.cycle_g.insert_vertex(0)
        # for i in range(1, 10):
        #     self.cycle_g.insert_vertex(i)
        #     self.cycle_g.update_adjacent(i - 1, i)
        # self.cycle_g.insert_vertex(10)
        # self.cycle_g.update_adjacent(9, 10)
        # self.cycle_g.update_adjacent(10, 0)
        self.graph = Graph()
        self.graph.insert_vertex(1)
        self.graph.insert_vertex(2)
        self.graph.insert_vertex(3)
        self.graph.insert_vertex(4)
        self.graph.insert_vertex(5)
        self.graph.update_adjacent(2, 1)
        self.graph.update_adjacent(3, 2)
        self.graph.update_adjacent(4, 3)
        self.graph.update_adjacent(5, 4)
        # self.graph.update_adjacent(1, 5)
        # res = graph.build_dag()

    def tearDown(self):
        pass

    # def test_dfs_dag(self):
    #     """ test dfs: DAG"""
    #     res_lst = self.graph.build_dag()
    #     print(res_lst)
    #     self.assertEqual(res_lst, list())

    def test_dfs_cycle(self):
        """ test dfs: cycle """
        res_lst = self.graph.build_dag()
        print(res_lst)
        self.assertEqual(len(res_lst), 11)
