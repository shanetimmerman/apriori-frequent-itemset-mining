import re
import os

for i in range(1, 5):
    print(f'Reformatting file input/combined_data_{i}.txt')
    #open the xml file for reading:
    file = open(f'input/combined_data_{i}.txt','r+')
    #convert to string:
    data = file.read()
    with open(f'input/data_{i}.txt','w+') as f:
        f.write(re.sub(r"\n(?=\d+:)","\n\n", data))
    file.close()

#open the xml file for reading:
print('Reformatting file input/qualifying.txt')
file = open(f'input/qualifying.txt','r+')
#convert to string:
data = file.read()
with open('input/data_5.txt','w+') as f:
    f.write(re.sub(r"\n(?=\d+:)","\n\n", data))
file.close()

print('Cleaning up files')
for i in range(1, 5):
    os.remove(f'input/combined_data_{i}.txt')
os.remove('input/data_5.txt','w+')
