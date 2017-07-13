import json
import apache_beam as beam
from apache_beam.metrics import Metrics

#self = 'l'
#logging = ''
#self.num_parse_error = Metrics.counter(self.__class__,'num parse errors')
file = open("C:/Users/Student/Documents/Over/2017-06-28_648","r")

file_read = file.readlines()
events = {}
for i in file_read:
    attr = i.strip('{').strip("}").replace('"',"").split(",")
    ### dma fix
    if(attr[15].split(":")[1] != "null"):
        while(':' not in attr[16]):
            attr[15] = attr[15] + ',' + attr[16]
            attr.remove(attr[16])
    ### JSON sub-group-check
    sub_group_check = 0
    while(sub_group_check < len(attr)):
        try:
            if(len(attr[sub_group_check].split(':')) == 1):
                attr[sub_group_check-1] = attr[sub_group_check-1]+ ","  + attr[sub_group_check]
                attr.remove(attr[sub_group_check])
                sub_group_check-=1
            sub_group_check+=1
        except:
            self.num_parse_error.inc()
            logging.info('subgroup parse error on %s',i)
    attr_dict ={}
    line_count = 0
    for k in attr:
        line_count +=1
        try:
            # if((len(k.split(':')) > 2) & ("user_properties" in k)):
            #     key_val = k.replace("user_properties:{","",1).split(":")
            # elif('event_properties' in k ) & (len(k.split(':')) > 2):
            #     key_val = k.replace("event_properties:{","",1).split(":")
            # else:
            #     key_val = k.strip('\n').strip('}').strip('{').split(":",1)
            if((len(k.split(':')) > 2) & ("{" in k)):
                key_val = k.replace('{', '', 1).split(':')
                attr_dict[key_val[1]] = key_val[2]
            else:
                key_val = k.strip('\n').split(':')
                attr_dict[key_val[0]] = key_val[1]
        except:
            self.num_parse_error.inc()
            logging.info('split parse error on %s', k)


