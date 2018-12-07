from parse_file import read_parse_file

begin = 1
end = 21

file_names = []
for i in range(1, 21):
    file_names.append('input{}.txt'.format(i))

file_names.append('input3.5.txt')
file_names.append('input3.7.txt')

for file_name in file_names:
    read_parse_file(file_name)
