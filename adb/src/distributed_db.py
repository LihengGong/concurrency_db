from parse_file import read_parse_file

single_num = 1

begin = single_num
end = single_num

file_names = []
for i in range(begin, end + 1):
    file_names.append('tests/input{}.txt'.format(i))

# file_names.append('input3.5.txt')
# file_names.append('input3.7.txt')

for file_name in file_names:
    read_parse_file(file_name)
