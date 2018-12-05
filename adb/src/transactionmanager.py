from datetime import datetime
from site import Sites
from dfs import Graph
from transaction import Transaction


class TransactionManager:
    def __init__(self):
        self.variable_map = dict()
        self.sites_map = dict()
        self.transaction_map = dict()
        self.transaction_sites_map = dict()
        self.op_wait_list = list()
        self.graph = Graph()
        for i in range(1, 21):
            self.variable_map[i] = list()
            if i % 2 == 0:
                for k in range(1, 11):
                    self.variable_map[i].append(k)
            else:
                self.variable_map[i].append(i % 10 + 1)

        for i in range(1, 11):
            self.sites_map[i] = Sites(i)

    def insert_site_to_trans_map(self, t_id, v_id):
        if t_id not in self.transaction_sites_map:
            self.transaction_sites_map[t_id] = list()

        if v_id % 2 == 1:
            self.transaction_sites_map[t_id].append(v_id % 10 + 1)
        else:
            for i in range(1, 11):
                self.transaction_sites_map[t_id].append(i)

    def start_transaction(self, trans_type, t_id):
        print('Start transaction ', t_id)
        if trans_type == 'RW':
            self.transaction_map[t_id] = Transaction(t_id, datetime.now(), 'RW')
            self.graph.insert_vertex(t_id)
        else:
            self.transaction_map[t_id] = Transaction(t_id, datetime.now(), 'RO')
            self.graph.insert_vertex(t_id)
        print()

    def end_transaction(self, t_id):
        if t_id not in self.transaction_map:
            self.graph.delete_vertex(t_id)
        else:
            is_delete_vert = True
            target = -1
            for i in range(len(self.op_wait_list)):
                if self.op_wait_list[i].trans_id == t_id:
                    target = i
            if target == -1:
                transaction = self.transaction_map[t_id]
                transaction.clear_op()

                for op in transaction.ops:
                    if op.op_type == 'read' and transaction.trans_type == 'RW':
                        if op.v_id % 2 == 1:
                            self.sites_map[op.v_id % 10 + 1].erase_lock(op.v_id, t_id)
                        else:
                            for i in range(1, 11):
                                self.sites_map[i].erase_lock(op.v_id, t_id)

                    if op.op_type == 'write' and transaction.trans_type == 'RO':
                        pass

    def run_operation(self, op):
        v_id = op.v_id
        trans = self.transaction_map[op.t_id]

        if v_id % 2 == 1:
            site = self.sites_map[v_id % 10 + 1]
            return site.commit(op, trans)
        else:
            for i in range(1, 11):
                if self.sites_map[i].status != 'fail' and not self.sites_map[i].commit(op, trans):
                    return False
            return True

    def abort(self, trans):
        t_id = trans.trans_id
        self.graph.delete_vertex(t_id)
        self.transaction_map.pop(t_id)
        for s_id in self.transaction_sites_map[t_id]:
            self.sites_map[s_id].remove_transaction(t_id)
        self.transaction_sites_map.pop(t_id)
        remove_lst = list()
        for w in self.op_wait_list:
            if w.trans_id == t_id:
                remove_lst.append(w)

        self.op_wait_list = [w for w in self.op_wait_list if w not in remove_lst]
        self.run_wait_list()

    """
    lst = [k for k in range(20)]
    
    print(lst)
    
    for i in range(len(lst) - 1, -1, -1):
        if lst[i] % 3 == 0:
            del lst[i]
    
    print(lst)
    """
    def run_wait_list(self):
        for i in range(len(self.op_wait_list) - 1, -1, -1):
            t_id = self.op_wait_list[i].trans_id
            trans = self.transaction_map[t_id]
            op = self.op_wait_list[i]

            if op.op_type == 'read':
                if self.read_op(t_id, op.v_ind):
                    print()
                if len(trans.ops) == 0:
                    print('')
                    self.end_transaction(op.trans_id)
                    print('')
            else:
                if self.write_op(op.trans_id, op.v_ind, op.v_val):
                    print('')
                    if len(trans.ops) == 0:
                        print('')
                        self.end_transaction(op.trans_id)
                        print('')

            del self.op_wait_list[i]

    def read_op(self, t_id, v_id):
        pass

    def write_op(self, t_id, v_id, v_val):
        pass

    def fail_site(self, s_id):
        print('Start to fail site ', s_id)
        site = self.sites_map[s_id]
        trans_lst = site.fail()
        if len(trans_lst) > 0:
            print('Site fails. Abort all transactions...')
            for t_id in trans_lst:
                print('aborting transaction T', t_id)
                self.abort(self.transaction_map[t_id])