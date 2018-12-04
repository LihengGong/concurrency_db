import datetime


class Variable:
    def __init__(self, v_ind, v_val):
        self.index = v_ind
        self.value = v_val
        self.committed_time = datetime.datetime.now()
        self.data_version = dict()
        self.data_version[self.committed_time] = self.value


class Sites:
    def __init__(self, s_id):
        self.site_id = s_id
        self.variables = dict()
        self.lock_table = dict()
        self.transaction_table = dict()
        for i in range(1, 21):
            if i % 2 == 0 or i % 10 + 1 == self.site_id:
                self.variables[i] = Variable(i, 10 * i)


