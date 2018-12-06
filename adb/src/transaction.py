class Operation:
    def __init__(self, tp, v_ind, time, tr_id, v_val=None):
        self.op_type = tp
        self.v_ind = v_ind
        self.time = time
        self.trans_id = tr_id
        if v_val:
            self.v_val = v_val

    def __repr__(self):
        return 'op-type:{}, v_id:{}, time:{}, t_id:{}, v_val:{}'.format(
            self.op_type, self.v_ind, self.time, self.trans_id, self.v_val
        )


class Transaction:
    def __init__(self, t_id, time, tp):
        self.trans_id = t_id
        self.time_stamp = time
        self.trans_type = tp
        self.ops = list()

    def insert_op(self, new_op):
        self.ops.append(new_op)

    def clear_op(self):
        self.ops = list()
