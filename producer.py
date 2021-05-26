from kafka import KafkaProducer
import json
import time
from datetime import datetime, timedelta


def get_registered_one(line):
    data = line.split(" ")
    our_date = str(data[0])+ " " + str(data[1])
    our_date = datetime.strptime(our_date, '%Y-%m-%d %H:%M:%S,%f')
    tmstmp = datetime.timestamp(our_date)
    tmstmp = int(tmstmp)/100
    return('@timestamp : '+ str(tmstmp)[:6]+ ', network : '+ str(data[3])+' token : '+str(data[5]))
        
    

        

def json_serializer(data):
    return json.dumps(data).encode('utf-8')



if __name__ == "__main__":
    f = open("/home/saleh/Desktop/new2.log", "r+")
    lines = []
    lines = f.readlines()
    f.close()
    f = open(f"/home/saleh/Desktop/new11.log", "w+")
    for i in range(0, 50000):
        try:
            registered_data = get_registered_one(lines[i])
            f.write(registered_data)
        except:
            continue 
    f.close()

    f = open(f"/home/saleh/Desktop/new12.log", "a+")
    for i in range(50001, 100000):
        try:
            registered_data = get_registered_one(lines[i])
            f.write(registered_data)
        except:
            continue 
    f.close()

    f = open(f"/home/saleh/Desktop/new13.log", "a+")
    for i in range(100001, 150000):
        try:
            registered_data = get_registered_one(lines[i])
            f.write(registered_data)
        except:
            continue 
    f.close()

    f = open(f"/home/saleh/Desktop/new14.log", "a+")
    for i in range(150000, len(lines)):
        try:
            registered_data = get_registered_one(lines[i])
            f.write(registered_data)
        except:
            continue 
    f.close()


    # for line in lines:
    #     try:
    #         f = open(f"/home/saleh/Desktop/new{int(j/50000)+5}.log", "w+")
    #         registered_data = get_registered_one(line)
    #         f.write(registered_data)
    #         f.close()
    #         j += 1
    #     except:
    #         continue   
    
        # 
        #     registered_data = get_registered_one(line)
        #     print(producer.send('log-test', registered_data))
        #     print(registered_data)
        # except:
        #     continue
    # producer.send('log-test', "Helloooooooooooooooooooo")


