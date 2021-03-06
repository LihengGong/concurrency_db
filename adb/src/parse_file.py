"""
Author: Liheng Gong(lg2848)
        Peiyue Yang(py570)

This module is responsible for read and parse inputs(test cases)

"""

import re
import transactionmanager

DEBUG_FLAG = False

def parse_single_file(file_name):
    total_line = get_total_line_numbr(file_name)
    print(total_line)
    read_parse_partial_file(file_name, total_line)

def read_parse_file(filename):
    trans_manager = transactionmanager.TransactionManager()
    print_header()
    print('begin processing for file ', filename)
    print_header()
    with open(filename) as file:
        for line in file:
            parse_line(line.strip(), trans_manager)
    print_header()
    print('end processing for file ', filename)
    print_header()
    print()
    print()

def print_header():
    stars = '*' * 50
    print(stars)

def get_total_line_numbr(filename):
    ln_num = -1
    with open(filename) as file:
        for ln_num, line in enumerate(file):
            pass
    return ln_num + 1

def read_parse_partial_file(filename, total_lines):
    next_start = 0
    batch_num = 0
    print_header()
    print('begin batch for file ', filename)
    print_header()
    while next_start < total_lines:
        batch_num += 1
        next_start = read_parse_file_from_line(filename, next_start)
        next_start += 1
    print_header()
    print('end of batch for file ', filename)
    print_header()
    print()

def read_parse_file_from_line(filename, start_line_number):
    trans_manager = transactionmanager.TransactionManager()
    start = start_line_number
    with open(filename) as file:
        for ln_num, line in enumerate(file):
            if ln_num >= start_line_number:
                start += 1
                parse_line(line.strip(), trans_manager)
    return start

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
        # print('begin trans num', trans_num)
        trans_manager.start_transaction('RW', trans_num)
    elif line.startswith('beginRO('):
        trans_num = find_transaction_number(line)
        trans_manager.start_transaction('RO', trans_num)
    elif line.startswith('W('):
        trans_num = find_transaction_number(line)
        v_ind = find_variable_ind(line)
        v_val = find_write_variable_val(line[2:-1])
        # print('write variable ind', v_ind, 'variable val', v_val)
        if trans_num in trans_manager.transaction_map:
            trans_manager.insert_site_to_trans_map(trans_num, v_ind)
            trans_manager.write_op(trans_num, v_ind, v_val)
        else:
            if DEBUG_FLAG:
                print('Wrong file format')
                print('trans num', trans_num)
                print('trans_manager.transaction_map', trans_manager.transaction_map)
                print('end of Wrong format')
    elif line.startswith('R('):
        # print('read line:', line)
        trans_num = find_transaction_number(line)
        v_ind = find_variable_ind(line)
        # print('read trans num', trans_num)
        if trans_num in trans_manager.transaction_map:
            trans_manager.insert_site_to_trans_map(trans_num, v_ind)
            trans_manager.read_op(trans_num, v_ind)
        else:
            if DEBUG_FLAG:
                    print('Wrong file format')
                    print('trans num', trans_num)
                    print('trans_manager.transaction_map', trans_manager.transaction_map)
                    print('end of Wrong format')
    elif line.startswith('end('):
        trans_num = find_transaction_number(line)
        # print('end trans num', trans_num)
        if trans_num in trans_manager.transaction_map:
            trans_manager.end_transaction(trans_num)
    elif line.startswith('recover('):
        site_number = find_pure_number(line)
        # print('recover', site_number)
        trans_manager.recover_site(site_number)
    elif line.startswith('fail('):
        site_number = find_pure_number(line)
        # print('fail', site_number)
        trans_manager.fail_site(site_number)
    elif line.startswith('dump('):
        if 'x' in line:
            v_ind = find_variable_ind(line)
            # print('dump x', v_ind)
            trans_manager.dump_single_val(v_ind)
        elif re.match(r'\d+', line):
            v_val = find_pure_number(line)
            # print('dump v', v_val)
            trans_manager.dump_single_site(v_val)
        else:
            # print('dump all')
            trans_manager.dump_all()
