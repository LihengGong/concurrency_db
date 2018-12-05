from datetime import datetime
from site import Sites, Lock
from dfs import Graph
from transaction import Transaction, Operation


class TransactionManager:
    def __init__(self):
        self.variable_site_map = dict()
        self.sites_map = dict()
        self.transaction_map = dict()
        self.transaction_sites_map = dict()
        self.op_wait_list = list()
        self.graph = Graph()
        for i in range(1, 21):
            self.variable_site_map[i] = list()
            if i % 2 == 0:
                for k in range(1, 11):
                    self.variable_site_map[i].append(k)
            else:
                self.variable_site_map[i].append(i % 10 + 1)

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
                        if op.v_ind % 2 == 1:
                            self.sites_map[op.v_ind % 10 + 1].erase_lock(op.v_ind, t_id)
                        else:
                            for i in range(1, 11):
                                self.sites_map[i].erase_lock(op.v_ind, t_id)

                    if op.op_type == 'write' and transaction.trans_type == 'RW':
                        self.run_operation(op)
            else:
                transaction = self.transaction_map[t_id]
                transaction.clear_op()

                for op in transaction.ops:
                    if op.op_type == 'read' and transaction.trans_type == 'RW':
                        if op.v_ind % 2 == 1:
                            self.sites_map[op.v_ind % 10 + 1].erase_lock(op.v_ind, t_id)
                        else:
                            for i in range(1, 11):
                                self.sites_map[i].erase_lock(op.v_ind, t_id)
                    if op.op_type == 'write' and transaction.trans_type == 'RW':
                        self.run_operation(op)

                if self.run_operation(self.op_wait_list[target]):
                    is_delete_vert = False
                else:
                    self.op_wait_list.pop(target)

            if is_delete_vert:
                res = self.transaction_map[t_id].trans_type == 'RW'
                if res:
                    self.graph.delete_vertex(t_id)
                print('Transaction ', t_id, ' committed, ')
                if target == -1:
                    print('normal commit')
                else:
                    print('unblocked and executed')
                self.transaction_map.pop(t_id)
                for item in self.transaction_sites_map[t_id]:
                    self.sites_map[item].remove_transaction(t_id)
                if res:
                    self.run_wait_list()

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
            del self.op_wait_list[i]

            if op.op_type == 'read':
                if self.read_op(t_id, op.v_ind):
                    print('Waiting op finished: T', t_id, op.op_type, 'x', op.v_ind)
                if len(trans.ops) == 0:
                    print('Waiting op to commit')
                    self.end_transaction(op.trans_id)
                    print('Waiting op commit done')
            else:
                if self.write_op(op.trans_id, op.v_ind, op.v_val):
                    print('Waiting op get lock: T', t_id, op.op_type, 'x', op.v_ind, '=', op.v_val)
                    if len(trans.ops) == 0:
                        print('Waiting op to commit')
                        self.end_transaction(op.trans_id)
                        print('Waiting op commit done')

    def read_op(self, t_id, v_id):
        if t_id not in self.transaction_map:
            print('Aborted Transaction in read_op')
            return True
        trans = self.transaction_map[t_id]
        res = True
        if trans.trans_type == 'RO':
            if v_id % 2 == 1:
                site = self.sites_map[v_id % 10 + 1]
                op1 = Operation('read', v_id, datetime.now(), t_id)
                site.add_operation(t_id, op1)
                op2 = Operation('read', v_id, datetime.now(), t_id)
                self.transaction_map[t_id].insert_op(op2)
                site.commit(op1, trans)
            else:
                for st in self.variable_site_map[v_id]:
                    site = self.sites_map[st]
                    op1 = Operation('read', v_id, datetime.now(), t_id)
                    site.add_operation(t_id, op1)
                    if site.commit(op1, trans):
                        break
                op2 = Operation('read', v_id, datetime.now(), t_id)
                self.transaction_map[t_id].insert_op(op2)
        else:
            if v_id % 2 == 1:
                site = self.sites_map[v_id % 10 + 1]
                op1 = Operation('read', v_id, datetime.now(), t_id)
                if site.insert_lock(v_id, Lock(v_id, t_id, 'read')):
                    site.add_operation(t_id, op1)
                    op2 = Operation('read', v_id, datetime.now(), t_id)
                    self.transaction_map[t_id].insert_op(op2)
                    site.commit(op1, trans)
                else:
                    self.op_wait_list.append(op1)
                    res = False
                    for lk in site.lock_table[v_id]:
                        if lk.lock_type == 'write' and lk.trans_id != t_id:
                            self.graph.update_adjacent(lk.trans_id, t_id)
                    self.check_deadlock()
            else:
                sites = self.variable_site_map[v_id]
                res = False
                op1 = Operation('read', v_id, datetime.now(), t_id)
                for st in sites:
                    site = self.sites_map[st]
                    op2 = Operation('read', v_id, datetime.now(), t_id)
                    if site.insert_lock(v_id, Lock(v_id, t_id, 'read')):
                        site.add_operation(t_id, op2)
                        res = True
                        site.commit(op2, trans)
                        break
                if not res:
                    self.op_wait_list.append(op1)
                    res = False
                    site = self.sites_map[1]
                    for st in self.sites_map.keys():
                        if self.sites_map[st].status == 'normal':
                            site = self.sites_map[st]
                            break

                    for lk in site.lock_table[v_id]:
                        if lk.lock_type == 'write' and lk.trans_id != t_id:
                            self.graph.update_adjacent(lk.trans_id, t_id)
                    self.check_deadlock()
                else:
                    op2 = Operation('read', v_id, datetime.now(), t_id)
                    self.transaction_map[t_id].insert_op(op2)
        return res

    def write_op(self, t_id, v_id, v_val):
        res = True
        if v_id % 2 == 1:
            op1 = Operation('write', v_id, v_val, datetime.now(), t_id)
            site = self.sites_map[v_id % 10 + 1]
            if site.insert_lock(v_id, Lock(v_id, t_id, 'write')):
                site.add_operation(t_id, op1)
                op2 = Operation('write', v_id, v_val, datetime.now(), t_id)
                self.transaction_map[t_id].insert_op(op2)
            else:
                self.op_wait_list.append(op1)
                res = False
                for lk in site.lock_table[v_id]:
                    if lk.trans_id != t_id:
                        self.graph.update_adjacent(lk.trans_id, t_id)
                self.check_deadlock()
        else:
            sites = self.variable_site_map[v_id]
            is_add_op = False
            op1 = Operation('write', v_id, v_val, datetime.now(), t_id)
            for st in sites:
                site = self.sites_map[st]
                op2 = Operation('write', v_id, v_val, datetime.now(), t_id)
                if site.status == 'normal':
                    if not site.insert_lock(v_id, Lock(v_id, t_id, 'write')):
                        break
                    else:
                        site.add_operation(t_id, op2)
                        is_add_op = True
            if is_add_op:
                for st in sites:
                    op2 = Operation('write', v_id, v_val, datetime.now(), t_id)
                    site = self.sites_map[st]
                    if site.status == 'recovery':
                        site.add_operation(t_id, op2)
                op3 = Operation('write', v_id, v_val, datetime.now(), t_id)
                self.transaction_map[t_id].insert_op(op3)
            else:
                self.op_wait_list.append(op1)
                res = False
                site = self.sites_map[1]
                for st in self.sites_map.keys():
                    if self.sites_map[st].status == 'normal':
                        site = self.sites_map[st]
                        break

                for lk in site.lock_table[v_id]:
                    if lk.trans_id != t_id:
                        self.graph.update_adjacent(lk.trans_id, t_id)
                self.check_deadlock()
                pass
        return res

    def check_deadlock(self):
        deadlock_trans = self.graph.build_dag()
        while len(deadlock_trans) > 0:
            res_trans = self.transaction_map[deadlock_trans[0]]
            for i in range(1, len(deadlock_trans)):
                trans1 = self.transaction_map[deadlock_trans[i]]
                res_trans = res_trans if res_trans.time_stamp > trans1.time_stamp else trans1
            print('Deadlock transactions: ', deadlock_trans)
            print('Aborting the youngest transaction ', res_trans.trans_id)
            self.abort(res_trans)
            deadlock_trans = self.graph.build_dag()

    def fail_site(self, s_id):
        print('Start to fail site ', s_id)
        site = self.sites_map[s_id]
        trans_lst = site.fail()
        if len(trans_lst) > 0:
            print('Site fails. Abort all transactions...')
            for t_id in trans_lst:
                print('aborting transaction T', t_id)
                self.abort(self.transaction_map[t_id])
        print('site', s_id, ' failed')

    def recover_site(self, s_id):
        print('Recover site', s_id)
        self.sites_map[s_id].recover()
        self.run_wait_list()
        print('Site', s_id, 'recovered')

    def dump_all(self):
        for i in range(1, 11):
            self.dump_single_val(i)

    def dump_single_val(self, v_id):
        sites = self.variable_site_map[v_id]
        for st in sites:
            print('Site ', st)
            st.dump_single(v_id)

    def dump_single_site(self, s_id):
        print('Dump site', s_id)
        self.sites_map[s_id].dump_all()

