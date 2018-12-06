from parse_file import read_parse_file, get_total_line_numbr, read_parse_partial_file


file_name = 'input1.txt'
# read_parse_file('input1.txt')
total_line = get_total_line_numbr(file_name)
print(total_line)

read_parse_partial_file(file_name, total_line)