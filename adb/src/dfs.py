class Vertices:
    def __init__(self, v_id):
        self.v_id = v_id
        self.adjacent = set()

    def __repr__(self):
        return 'v_id: {}, adjacent list: {}'.format(self.v_id, self.adjacent)

    def insert_v(self, vtx):
        self.adjacent.add(vtx.v_id)

    def delete_v(self, v_id):
        if v_id in self.adjacent:
            self.adjacent.remove(v_id)
        # k = 0
        # for v in self.adjacent:
        #     if v_id == v:
        #         break
        #     k += 1
        # if len(self.adjacent) > 0 and k < len(self.adjacent):
        #     self.adjacent.pop(k)


class Graph:
    def __init__(self):
        self.vertices = list()

    def insert_vertex(self, v_id):
        self.vertices.append(Vertices(v_id))

    def retrieve_vertex(self, v_id):
        for v in self.vertices:
            if v.v_id == v_id:
                return v

    def update_adjacent(self, v_id, a_id):
        # if (v_id == a_id):
        #     print('update_adjacent vid==aid=', v_id, a_id)
        vertex = self.retrieve_vertex(v_id)
        adj_v = self.retrieve_vertex(a_id)
        if adj_v not in vertex.adjacent:
            vertex.insert_v(adj_v)

    def delete_vertex(self, a_id):
        k = 0
        for v in self.vertices:
            if v.v_id == a_id:
                break
            k += 1
        # print('before delete vertex: k=', k)
        # print('before delete vertex: vertices:', self.vertices)
        self.vertices.pop(k)
        for v in self.vertices:
            v.delete_v(a_id)
        # print('after delete vertex: k=', k)
        # print('after delete vertex: vertices:', self.vertices)

    def find_index(self, vertices, vtx):
        k = 0
        # print('find-index: vertices:', vertices, 'vtx:', vtx)
        for v in vertices:
            if vtx == v.v_id:
                return k
            k += 1

    def build_dag(self):
        is_visited = [0 for i in range(len(self.vertices))]
        v_of_cycle = set()
        for i in range(len(is_visited)):
            if is_visited[i] == 0:
                self.dfs(i, is_visited, v_of_cycle)

        if len(v_of_cycle) == 0:
            return list()
        else:
            res_path = list()
            for v in v_of_cycle:
                res_path.append(self.vertices[v].v_id)
            return res_path

    def dfs(self, v_id, is_visited, res_set):
        if is_visited[v_id] == 1:
            res_set.add(v_id)
            return True

        is_visited[v_id] = 1
        has_cycle = False

        # print('dfs: adjacent: ', self.vertices[v_id].adjacent)
        # print('dfs: vertices:', self.vertices)
        for v in self.vertices[v_id].adjacent:
            # ind = self.vertices.index(v)
            ind = self.find_index(self.vertices, v)
            # print('dfs: ind=', ind)
            has_cycle = self.dfs(ind, is_visited, res_set)
            if has_cycle:
                if v_id in res_set:
                    return False
                else:
                    res_set.add(v_id)
                    return True

        is_visited[v_id] = 2
        return has_cycle


# code for testing
if __name__ == '__main__':
    """ test """
    graph = Graph()
    graph.insert_vertex(1)
    graph.insert_vertex(2)
    graph.insert_vertex(3)
    graph.insert_vertex(4)
    graph.insert_vertex(5)
    graph.update_adjacent(2, 1)
    graph.update_adjacent(3, 2)
    graph.update_adjacent(4, 3)
    graph.update_adjacent(5, 4)
    graph.update_adjacent(1, 5)
    res = graph.build_dag()
    print(res)
