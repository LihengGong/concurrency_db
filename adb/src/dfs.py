class Vertices:
    def __init__(self, v_id):
        self.v_id = v_id
        self.adjacent = list()

    def insert_v(self, vtc):
        self.adjacent.append(vtc)

    def delete_v(self, v_id):
        k = 0
        for v in self.adjacent:
            k += 1
            if v_id == v.v_id:
                break
        self.adjacent.remove(k)


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
        vertex = self.retrieve_vertex(v_id)
        adjacent = self.retrieve_vertex(a_id)
        if adjacent not in vertex.adjacent:
            vertex.insert_v(adjacent)

    def delete_vertex(self, a_id):
        k = 0
        for v in self.vertices:
            k += 1
            if v.v_id == a_id:
                break
        self.vertices.remove(k)
        for v in self.vertices:
            v.delete_v(k)

    def build_dag(self):
        is_visited = [False for i in range(len(self.vertices))]
        v_of_cycle = set()
        for i in range(len(is_visited)):
            if is_visited[i] is False:
                self.dfs(i, is_visited, v_of_cycle)

        # if len(v_of_cycle) == 0:
        #     return list()
        # else:
        res_path = list()
        for v in v_of_cycle:
            res_path.append(self.vertices[v].v_id)
        return res_path

    def dfs(self, v_id, is_visited, res):
        if is_visited[v_id]:
            res.add(v_id)
            return True

        is_visited[v_id] = True
        has_cycle = False

        for v in self.vertices[v_id].adjacent:
            ind = self.vertices.index(v)
            has_cycle = self.dfs(ind, is_visited, res)
            if has_cycle:
                if v_id in res:
                    return False
                else:
                    res.add(v_id)
                    return True

        return has_cycle


if __name__ == '__main__':
    """ test """
    graph = Graph()
    graph.insert_vertex(1)
    graph.insert_vertex(2)
    graph.insert_vertex(3)
    graph.insert_vertex(4)
    graph.insert_vertex(5)
    graph.update_adjacent(1, 2)
    graph.update_adjacent(2, 3)
    graph.update_adjacent(3, 4)
    graph.update_adjacent(4, 5)
    graph.update_adjacent(5, 1)
    res = graph.build_dag()
    print(res)
