"""
Author: Liheng Gong(lg2848)
        Peiyue Yang(py570)
"""

class Operation:
    """
    Class for operations
    """
    def __init__(self, tp, v_ind, time, tr_id, v_val=None):
        self.op_type = tp
        self.v_ind = v_ind
        self.time = time
        self.trans_id = tr_id
        self.is_after_recovery = True
        self.v_val = None
        if v_val:
            self.v_val = v_val

    def __repr__(self):
        return 'op-type:{}, v_id:{}, time:{}, t_id:{}, is_bef: {}, v_val:{}....#'.format(
            self.op_type, self.v_ind, self.time, self.trans_id, self.is_after_recovery, self.v_val
        )


class Transaction:
    """
    Class that represents transaction
    """
    def __init__(self, t_id, time, tp):
        self.trans_id = t_id
        self.time_stamp = time
        self.trans_type = tp
        self.operations = list()

    def insert_op(self, new_op):
        self.operations.append(new_op)

    def clear_op(self):
        self.operations = list()
    
    def __repr__(self):
        return 'tid: {}, trans_type: {}, time: {}, ops: {}'.format(
            self.trans_id, self.trans_type, self.time_stamp, self.operations
        )
