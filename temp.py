with open('areas.json') as f:
    data = f.readlines()

length = 0
counter = 0
for line in data:
    counter += 1
    if len(line) > length:
        length = len(line)
        max_line_number = counter
        
    
    
print ('length = ', length)
print ('line number = ', max_line_number)
    
