import re
import transactionmanager


def read_parse_file(filename):
    trans_manager = transactionmanager.TransactionManager()
    with open(filename) as file:
        for line in file:
            parse_line(line.strip(), trans_manager)


def find_transaction_number(line):
    lst_res = re.findall(r'T\d+', line)
    return int(lst_res[0][1:]) if len(lst_res) > 0 else None


def find_variable_ind(line):
    lst_res = re.findall(r'x\d+', line)
    return int(lst_res[0][1:]) if len(lst_res) > 0 else None


def find_write_variable_val(line):
    lst_res = line.split(',')
    return int(lst_res[2].strip()) if len(lst_res) > 2 else None


def find_pure_number(line):
    lst_res = re.findall(r'\d+', line)
    return int(lst_res[0]) if len(lst_res) > 0 else None


def parse_line(line, trans_manager):
    if line.startswith('begin('):
        trans_num = find_transaction_number(line)
        print('begin trans num', trans_num)
        # transaction.start_cur_trans('typeRW', 0)
        trans_manager.start_transaction('RW', trans_num)
    elif line.startswith('beginRO('):
        trans_num = find_transaction_number(line)
        # transaction.start_cur_trans('typeRO', 0)
        trans_manager.start_transaction('RO', trans_num)
    elif line.startswith('W('):
        trans_num = find_transaction_number(line)
        v_ind = find_variable_ind(line)
        v_val = find_write_variable_val(line[2:-1])
        print('write variable ind', v_ind, 'variable val', v_val)
        if trans_num in trans_manager.transaction_map:
            trans_manager.insert_site_to_trans_map(trans_num, v_ind)
            trans_manager.write_op(trans_num, v_ind, v_val)
        else:
            print('Wrong file format')
        # transaction.write_operation()
    elif line.startswith('R('):
        print('read line:', line)
        trans_num = find_transaction_number(line)
        v_ind = find_variable_ind(line)
        print('read trans num', trans_num)
        if trans_num in trans_manager.transaction_map:
            trans_manager.insert_site_to_trans_map(trans_num, v_ind)
            trans_manager.read_op(trans_num, v_ind)
        # transaction.read_operation()
    elif line.startswith('end('):
        trans_num = find_transaction_number(line)
        print('end trans num', trans_num)
        if trans_num in trans_manager.transaction_map:
            trans_manager.end_transaction(trans_num)
        # transaction.end_transaction()
    elif line.startswith('recover('):
        site_number = find_pure_number(line)
        print('recover', site_number)
        trans_manager.recover_site(site_number)
        # transaction.recover_site()
    elif line.startswith('fail('):
        site_number = find_pure_number(line)
        print('fail', site_number)
        trans_manager.fail_site(site_number)
        # transaction.fail_site()
    elif line.startswith('dump('):
        if 'x' in line:
            v_ind = find_variable_ind(line)
            print('dump x', v_ind)
            trans_manager.dump_single_val(v_ind)
        elif re.match(r'\d+', line):
            v_val = find_pure_number(line)
            print('dump v', v_val)
            trans_manager.dump_single_site(v_val)
        else:
            print('dump all')
            trans_manager.dump_all()
        # transaction.dump_info()
