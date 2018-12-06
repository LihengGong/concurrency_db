from datetime import datetime
from collections import deque
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
        v_id = op.v_ind
        trans = self.transaction_map[op.trans_id]

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
        print('run wait list, wait list:', self.op_wait_list)
        self.op_wait_list.reverse()
        for i in range(len(self.op_wait_list) - 1, -1, -1):
            t_id = self.op_wait_list[i].trans_id
            print('run-wait-list, t_id=', t_id)
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
        self.op_wait_list.reverse()

    def read_op(self, t_id, v_id):
        print('in read op, tid', t_id, 'vid', v_id)
        if t_id not in self.transaction_map:
            print('Aborted Transaction in read_op')
            return True
        trans = self.transaction_map[t_id]
        res = True

        if trans.trans_type == 'RO':
            if v_id % 2 == 1:
                site = self.sites_map[v_id % 10 + 1]
                op1 = Operation('read', v_id, datetime.now(), t_id, None)
                site.add_operation(t_id, op1)
                op2 = Operation('read', v_id, datetime.now(), t_id, None)
                self.transaction_map[t_id].insert_op(op2)
                site.commit(op1, trans)
            else:
                for st in self.variable_site_map[v_id]:
                    site = self.sites_map[st]
                    op1 = Operation('read', v_id, datetime.now(), t_id, None)
                    site.add_operation(t_id, op1)
                    if site.commit(op1, trans):
                        break
                op2 = Operation('read', v_id, datetime.now(), t_id, None)
                self.transaction_map[t_id].insert_op(op2)
        else:
            if v_id % 2 == 1:
                site = self.sites_map[v_id % 10 + 1]

                op1 = Operation('read', v_id, datetime.now(), t_id, None)
                cur_res = site.insert_lock(v_id, Lock(v_id, t_id, 'read'))
                print('read op, odd, sites map', site, ' res of insert-lock:', cur_res)
                if cur_res:
                    site.add_operation(t_id, op1)
                    op2 = Operation('read', v_id, datetime.now(), t_id, None)
                    self.transaction_map[t_id].insert_op(op2)
                    site.commit(op1, trans)
                else:
                    self.op_wait_list.append(op1)
                    res = False
                    for lk in site.lock_table[v_id]:
                        if lk.lock_type == 'write' and lk.trans_id != t_id:
                            print('I graph add edge:', lk.trans_id, '---', t_id)
                            self.graph.update_adjacent(lk.trans_id, t_id)
                    self.check_deadlock()
            else:
                sites = self.variable_site_map[v_id]
                print('read op, even, sites map', sites)
                res = False
                op1 = Operation('read', v_id, datetime.now(), t_id, None)
                for st in sites:
                    site = self.sites_map[st]
                    op2 = Operation('read', v_id, datetime.now(), t_id, None)
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
                            print('II graph add edge:', lk.trans_id, '---', t_id)
                            self.graph.update_adjacent(lk.trans_id, t_id)
                    self.check_deadlock()
                else:
                    op2 = Operation('read', v_id, datetime.now(), t_id, None)
                    self.transaction_map[t_id].insert_op(op2)
        return res

    def write_op(self, t_id, v_id, v_val):
        print('in write op, tid', t_id, 'vid', v_id, 'newval=', v_val)
        res = True
        if v_id % 2 == 1:
            op1 = Operation('write', v_id, datetime.now(), t_id, v_val)
            site = self.sites_map[v_id % 10 + 1]
            if site.insert_lock(v_id, Lock(v_id, t_id, 'write')):
                site.add_operation(t_id, op1)
                op2 = Operation('write', v_id, datetime.now(), t_id, v_val)
                self.transaction_map[t_id].insert_op(op2)
            else:
                self.op_wait_list.append(op1)
                res = False
                for lk in site.lock_table[v_id]:
                    if lk.trans_id != t_id:
                        print('III graph add edge:', lk.trans_id, '---', t_id)
                        self.graph.update_adjacent(lk.trans_id, t_id)
                self.check_deadlock()
        else:
            sites = self.variable_site_map[v_id]
            is_add_op = False
            op1 = Operation('write', v_id, datetime.now(), t_id, v_val)
            for st in sites:
                site = self.sites_map[st]
                op2 = Operation('write', v_id, datetime.now(), t_id, v_val)
                if site.status == 'normal':
                    if not site.insert_lock(v_id, Lock(v_id, t_id, 'write')):
                        break
                    else:
                        site.add_operation(t_id, op2)
                        is_add_op = True
            if is_add_op:
                for st in sites:
                    op2 = Operation('write', v_id, datetime.now(), t_id, v_val)
                    site = self.sites_map[st]
                    if site.status == 'recovery':
                        site.add_operation(t_id, op2)
                op3 = Operation('write', v_id, datetime.now(), t_id, v_val)
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
                        print('IV graph add edge:', lk.trans_id, '---', t_id)
                        self.graph.update_adjacent(lk.trans_id, t_id)
                self.check_deadlock()
                pass
        return res

    def check_deadlock(self):
        deadlock_trans = self.graph.build_dag()
        print('detect dag: deadlock_trans', deadlock_trans)

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
        # sites = self.sites_map[v_id]
        for st in sites:
            print('Site ', st)
            site = self.sites_map[st]
            site.dump_single(v_id)
            # st.dump_single(v_id)

    def dump_single_site(self, s_id):
        print('Dump site', s_id)
        self.sites_map[s_id].dump_all()


class Sites:
    def __init__(self, s_id):
        self.site_id = s_id
        self.status = 'normal'
        self.variables = dict()
        self.lock_table = dict()
        self.transaction_table = dict()
        for i in range(1, 21):
            if i % 2 == 0 or i % 10 + 1 == self.site_id:
                self.variables[i] = Variable(i, 10 * i)
                self.lock_table[i] = list()

    def __repr__(self):
        return 'site id = {}, status = {}'.format(self.site_id, self.status)

    def update_variable_lock(self, ind, lock):
        pass
        # self.lock_table[ind].append(lock)

    def insert_lock(self, v_ind, lock):
        if self.status == 'fail':
            return False
        elif self.status == 'recovery':
            if lock.lock_type == 'read' and v_ind % 2 == 0:
                return False

        variable_locks = self.lock_table[v_ind]
        if lock.lock_type == 'read':
            if len(variable_locks) == 0:
                variable_locks.append(lock)
                self.lock_table[v_ind] = variable_locks
                return True
            else:
                res = False
                for v_l in variable_locks:
                    if v_l.lock_type == 'write':
                        res = True
                        break
                if not res:
                    variable_locks.append(lock)
                    self.lock_table[v_ind] = variable_locks
                return not res
        else:
            print('insert lock, len of variable-locks:', len(variable_locks))
            if len(variable_locks) > 1:
                return False
            elif len(variable_locks) == 1:
                if variable_locks[0].trans_id == lock.trans_id:
                    variable_locks.append(lock)
                    self.lock_table[v_ind] = variable_locks
                    return True
                else:
                    return False
            else:
                variable_locks.append(lock)
                self.lock_table[v_ind] = variable_locks
                return True

    def erase_lock(self, v_ind, t_id):
        locks = self.lock_table[v_ind]

        for lk in locks:
            if lk.trans_id == t_id:
                locks.remove(lk)
                break

        self.lock_table[v_ind] = locks

    def remove_transaction(self, t_id):
        if t_id in self.transaction_table:
            self.transaction_table.pop(t_id)
            for k in self.lock_table.keys():
                lst = list()
                for lk in self.lock_table[k]:
                    if lk.trans_id == t_id:
                        lst.append(lk)
                res_lst = [item for item in self.lock_table[k] if item not in lst]
                self.lock_table[k] = res_lst
            return True
        else:
            return False

    def add_operation(self, t_id, operation):
        if t_id not in self.transaction_table:
            self.transaction_table[t_id] = deque()
        self.transaction_table[t_id].append(operation)

    def fail(self):
        self.status = 'fail'
        for i in range(1, 21):
            if i % 2 == 0 or i % 10 + 1 == self.site_id:
                self.lock_table[i] = list()

        self.transaction_table = dict()
        return list(self.transaction_table.keys())

    def recover(self):
        self.status = 'recovery'

    def dump_all(self):
        for i in range(1, 21):
            if i % 2 == 0 or i % 10 + 1 == self.site_id:
                print('Variable x', self.variables[i].index, ' : ', self.variables[i].value)

    def dump_single(self, ind):
        print('Variable x', self.variables[ind].index, ' : ', self.variables[ind].value)

    def commit(self, operation, transaction):
        print('site commit, type is ', transaction.trans_type)
        v_ind = operation.v_ind
        if transaction.trans_type == 'RO':
            if operation.op_type == 'read':
                if v_ind % 2 == 1:
                    if self.status == 'fail':
                        return False

                    print('Variable x', self.variables[v_ind].index,
                          ' from T', transaction.trans_id, ' READ value ',
                          self.variables[v_ind].retrieve_newest(datetime.now()),
                          ' at site', self.site_id)
                    return True
                else:
                    if self.status == 'normal':
                        print('Variable x', self.variables[v_ind].index,
                              ' from T', transaction.trans_id, ' READ value ',
                              self.variables[v_ind].retrieve_newest(datetime.now()),
                              ' at site', self.site_id)
                        return True
                    return False
        else:
            if operation.op_type == 'read':
                res = False
                for lk in self.lock_table[v_ind]:
                    if lk.lock_type == 'write' and lk.trans_id != transaction.trans_id:
                        res = True
                        break
                if res:
                    return False

                if v_ind % 2 == 1:
                    if self.status == 'fail':
                        return False

                    print('Variable x', self.variables[v_ind].index, ' from T',
                          transaction.trans_id, ' READ value ',
                          self.variables[v_ind].value, ' at site', self.site_id)
                    return True
                else:
                    if self.status == 'normal':
                        print('Variable x', self.variables[v_ind].index, ' from T',
                              transaction.trans_id, ' READ value ',
                              self.variables[v_ind].value, ' at site', self.site_id)
                        return True
                    return False
            else:
                res = True
                for lk in self.lock_table[v_ind]:
                    if lk.lock_type == 'write' and lk.trans_id != transaction.trans_id:
                        res = False
                        break
                if res:
                    return False

                if self.status == 'fail':
                    return False
                elif self.status == 'recovery':
                    self.variables[v_ind].set_value(operation.v_val)
                    self.status = 'normal'
                    print('Variable x', self.variables[v_ind].index, ' from T',
                          transaction.trans_id, ' WRITE value ',
                          self.variables[v_ind].value, ' at site', self.site_id)
                    print('Site ', self.site_id, ' become NORMAL because T',
                          transaction.trans_id, 'WRITE x',
                          )
                    self.erase_lock(v_ind, transaction.trans_id)
                    return True
                else:
                    self.variables[v_ind].set_value(operation.v_val)
                    print('Variable x', self.variables[v_ind].index, ' from T',
                          transaction.trans_id, ' WRITE value ',
                          self.variables[v_ind].value, ' at site', self.site_id)
                    self.erase_lock(v_ind, transaction.trans_id)
                    return True
        return False


class Variable:
    def __init__(self, v_ind, v_val):
        self.index = v_ind
        self.value = v_val
        self.start_time = datetime.now()
        self.committed_delta_time = datetime.now() - self.start_time
        self.data_version = dict()
        self.data_version[self.committed_delta_time] = self.value

    def set_value(self, v_val):
        self.committed_delta_time = datetime.now() - self.start_time
        # delta_time = datetime.datetime.now() - self.start_time
        self.data_version[self.committed_delta_time] = v_val
        self.value = v_val

    def retrieve_newest(self, time):
        delta_time = time - self.start_time
        latest = [k for k in self.data_version.keys() if k < delta_time]
        return self.data_version[latest[0]] if len(latest) > 0 else None


class Lock:
    def __init__(self, v_ind, t_id, l_type):
        self.val_ind = v_ind
        self.trans_id = t_id
        self.lock_type = l_type







