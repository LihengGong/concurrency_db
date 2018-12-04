import re
import transaction


def read_parse_file(filename):
    with open(filename) as file:
        for line in file:
            parse_line(line.strip())


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


def parse_line(line):
    if line.startswith('begin('):
        trans_num = find_transaction_number(line)
        print('begin trans num', trans_num)
        transaction.start_cur_trans('typeRW', 0)
    elif line.startswith('beginRO('):
        transaction.start_cur_trans('typeRO', 0)
    elif line.startswith('W('):
        v_ind = find_variable_ind(line)
        v_val = find_write_variable_val(line[2:-1])
        print('write variable ind', v_ind, 'variable val', v_val)
        transaction.write_operation()
    elif line.startswith('R('):
        trans_num = find_transaction_number(line)
        print('read trans num', trans_num)
        transaction.read_operation()
    elif line.startswith('end('):
        trans_num = find_transaction_number(line)
        print('end trans num', trans_num)
        transaction.end_transaction()
    elif line.startswith('recover('):
        site_number = find_pure_number(line)
        print('recover', site_number)
        transaction.recover_site()
    elif line.startswith('fail('):
        site_number = find_pure_number(line)
        print('fail', site_number)
        transaction.fail_site()
    elif line.startswith('dump('):
        if 'x' in line:
            v_ind = find_variable_ind(line)
            print('dump x', v_ind)
        elif re.match(r'\d+', line):
            v_val = find_pure_number(line)
            print('dump v', v_val)
        else:
            print('dump all')
        transaction.dump_info()


if __name__ == '__main__':
    read_parse_file('input1.txt')
