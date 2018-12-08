"""
Author: Liheng Gong(lg2848)
        Peiyue Yang(py570)
"""
import sys
from parse_file import read_parse_file

if (len(sys.argv) == 1):
    print('Please specify input file name')

for i in range(1, len(sys.argv)):
    read_parse_file(sys.argv[i])

# single_num = 21

# begin = single_num
# end = single_num + 1

# file_names = []
# for i in range(begin, end):
#     file_names.append('tests/input{}.txt'.format(i))

# # file_names.append('tests/input3.5.txt')
# # file_names.append('tests/input3.7.txt')

# for file_name in file_names:
#     read_parse_file(file_name)
