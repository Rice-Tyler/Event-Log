import json
file = open("C:/Users/Student/Documents/Over/parse","r")
file_read = file.readlines()
for i in file_read:
    attr = i.strip('{').strip("}").replace('"',"").split(",")
    if(attr[15].split(":")[1] != "null"):
        attr[15] =attr[15] + attr[16]
        attr.remove(attr[16])
    attr_dict ={}
    for k in attr:
        print(k)
        key_val = k.split(":",1)
        print(key_val)
        attr_dict[key_val[0]] = key_val[1]
    print(attr_dict)

