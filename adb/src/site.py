from datetime import datetime
from collections import deque


class Variable:
    def __init__(self, v_ind, v_val):
        self.index = v_ind
        self.value = v_val
        self.start_time = datetime.now()
        self.committed_delta_time = datetime.now() - self.start_time
        self.data_version = dict()
        self.data_version[self.committed_delta_time.total_seconds()] = self.value

    def set_value(self, v_val):
        self.committed_delta_time = datetime.now() - self.start_time
        # delta_time = datetime.datetime.now() - self.start_time
        self.data_version[self.committed_delta_time.total_seconds()] = v_val
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



