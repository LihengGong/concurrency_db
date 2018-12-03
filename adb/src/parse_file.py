from transaction import start_cur_trans


def read_parse_file(filename):
    with open(filename) as file:
        for line in file:
            parse_line(line.strip())


def parse_line(line):
    if line.startswith('begin('):
        start_cur_trans('typeRW', 0)
    elif line.startswith('beginRO('):
        start_cur_trans('typeRO', 0)
    elif line.startswith('W('):
        print('transaction w')
    elif line.startswith('R('):
        print('transaction r')
    elif line.startswith('end('):
        print('end trans')
    elif line.startswith('recover('):
        print('recover')
    elif line.startswith('fail('):
        print('fail')
    elif line.startswith('dump('):
        print('dump')


if __name__ == '__main__':
    read_parse_file('input1.txt')
