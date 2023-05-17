"""
This scripts reads a binary CANBus log and parses the 
fields of interest within that logs and saves them as a csv file in the SAVE_DIR folder
for further data analysis
"""

import sys
import struct
import os
import datetime
import csv
from bitstring import Bits


SAVE_DIR = './data/csv_logs'
DATA_DICT = {777: 'das_objects', 921: 'autopilot', 273: 'accelerometer', 257: 'angular_velocity', 79: 'gps', 599: 'speed'}
AP_STATE_DICT = {0:'DISABLED', 1:'UNAVAILABLE' , 2:'AVAILABLE', 3:'ACTIVE_NOMINAL', 4:'ACTIVE_RESTRICTED', 5:'ACTIVE_NAV', 8:'ABORTING' , 9:'ABORTED'}
ACC_SCALE = 0.00125
YAW_SCALE = 0.0001
PITCH_ROLL_SCALE = 0.00025
SPEED_SCALE = 0.08
SPEED_OFFSET = -40.0
GNNS_FACTOR = 1e-6

MAX_SR = 1.2

#Helper function for the new add:
def bin_to_dec(str1):
    sum = 0
    lenth = len(str1)  
    for i in range(1,lenth):        
        if str1[i] == '1':    
            save = 2**(lenth-i-1)   
            sum = sum+save      
    
    if str1[0] == '1':    
        return sum-2**(lenth-1)
    else:
        return +sum

if len(sys.argv) != 2:
    print("To run the script follow the following: python can_reader.py <infile> ")
    exit(1)

csv_filedsnames = ['timestamp', 'long_acc', 'lat_acc', 'vert_acc', 'acc_unit', 'yaw_rate', 'pitch_rate', 'roll_rate', 'gyro_unit', 'lat', 'long', 'speed', 'speed_unit', 'AP_status']
lastSyncTime = 0

outputfile = None
base = os.path.basename(sys.argv[1])
outputFilename = os.path.splitext(base)[0] + '.csv'
output_path = os.path.join(SAVE_DIR, outputFilename)

def parse_and_insert(frameid, payload, epoch_time, epoch_dict, convert_time=False):
    if convert_time:
        epoch_dict[epoch_time][0] = datetime.datetime.fromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    if frameid == 273:
        long_acc = ACC_SCALE * int.from_bytes(payload[0:2], 'little', signed=True)
        lat_acc = ACC_SCALE * int.from_bytes(payload[2:4], 'little', signed=True)
        vert_acc = ACC_SCALE * int.from_bytes(payload[4:6], 'little', signed=True)
        epoch_dict[epoch_time][1:5] = long_acc, lat_acc, vert_acc, 'm/s^2'
    
    elif frameid == 257:
        #print(bin(payload[3]))
        yaw_rate = YAW_SCALE * int.from_bytes(payload[0:2], 'little', signed=True)
        #New Adds Starts here
        pitch_str = '{0:08b}'.format(payload[3])[1:] + '{0:08b}'.format(payload[2])
        roll_str = '{0:08b}'.format(payload[5])[2:] + '{0:08b}'.format(payload[4]) + '{0:08b}'.format(payload[3])[0]
        pitch_int = bin_to_dec(pitch_str)
        roll_int = bin_to_dec(roll_str)
        pitch_rate = PITCH_ROLL_SCALE * pitch_int
        roll_rate = PITCH_ROLL_SCALE * roll_int
        # New Adds finishes here
        #pitch_rate = PITCH_ROLL_SCALE * (Bits(bin=('{0:08b}'.format(payload[3])[1:] + '{0:08b}'.format(payload[2]))).int)
        #roll_rate = PITCH_ROLL_SCALE * (Bits(bin=('{0:08b}'.format(payload[5])[2:] + '{0:08b}'.format(payload[4]) + '{0:08b}'.format(payload[3])[0])).int)
        epoch_dict[epoch_time][5:9] = yaw_rate, pitch_rate, roll_rate, 'rad/s' 
    
    elif frameid == 599:
        speed = SPEED_SCALE * int('{0:08b}'.format(payload[2]) + '{0:08b}'.format(payload[1])[:4], 2) + SPEED_OFFSET
        epoch_dict[epoch_time][11:13] = speed, 'KPH'
    
    elif frameid == 79:
        #New Add start here
        lat_str = '{0:08b}'.format(payload[3])[4:] + '{0:08b}'.format(payload[2]) + '{0:08b}'.format(payload[1]) + '{0:08b}'.format(payload[0])
        long_str = '{0:08b}'.format(payload[6]) + '{0:08b}'.format(payload[5]) + '{0:08b}'.format(payload[4]) + '{0:08b}'.format(payload[3])[:4]
        lat_int = bin_to_dec(lat_str)
        long_int = bin_to_dec(long_str) 
        lat = GNNS_FACTOR * lat_int
        long = GNNS_FACTOR * long_int
        #New Add finishes here        
        #lat = GNNS_FACTOR * (Bits(bin=('{0:08b}'.format(payload[3])[4:] + '{0:08b}'.format(payload[2]) + '{0:08b}'.format(payload[1]) + '{0:08b}'.format(payload[0]))).int)
        #long = GNNS_FACTOR * (Bits(bin=('{0:08b}'.format(payload[6]) + '{0:08b}'.format(payload[5]) + '{0:08b}'.format(payload[4]) + '{0:08b}'.format(payload[3])[:4])).int)
        epoch_dict[epoch_time][9:11] = lat, long

    elif frameid == 921:
        ap_state = int('{0:08b}'.format(payload[0])[4:], 2)
        epoch_dict[epoch_time][13] = ap_state

with open(sys.argv[1], mode='rb') as file:
    #File header should be 22 bytes
    headerData = file.read(22)
    if (len(headerData) == 22):
        #Check to see if our header matches what we expect        
        if (headerData == b'CANSERVER_v2_CANSERVER'):
            outputfile = open(output_path, mode='w')
            csv_writer= csv.writer(outputfile, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
            csv_writer.writerow(csv_filedsnames)
            pass
        else:
            print("Not a valid CANServer v2 file.  Unable to convert", file=sys.stderr)
            exit(1)
    #current_row = ['NA'] * len(csv_filedsnames)
    epoch_dict = {}
    min_epoch = 0
    while True:
        #Look for the start byte
        byteRead = file.read(1)
        if len(byteRead) == 1:
            if (byteRead == b'C'):
                #check to see if we have a header.
                goodheader = False
                
                #read 21 more bytes
                possibleHeader = file.read(21)
                if (len(possibleHeader) == 21):
                    if (possibleHeader == b'ANSERVER_v2_CANSERVER'):
                        #we found a header (this might have been because of just joining multiple files togeather)
                        goodheader = True
                        pass

                if (goodheader):
                    #header was valid.  Just skip on ahead
                    pass
                else:
                    #we didn't see the header we expected.  Seek backwards the number of bytes we read
                    file.seek(-len(possibleHeader), 1)                    
            elif (byteRead == b'\xcd'):
                #this is a mark message.  The ASC format doesn't seem to have any comments or anything so we can't directly convert this mark
                #Instead we create a new output file with the markstring as part of its filename
                marksize = file.read(1)
                marksize = int.from_bytes(marksize, 'big')
                markdata = file.read(marksize)

                markString = markdata.decode("ascii")
                print("Parsing the log with markString: ", markString)

            elif (byteRead == b'\xce'):
                #this is running time sync message.
                timesyncdata = file.read(8)

                if len(timesyncdata) == 8:
                    lastSyncTime = struct.unpack('<Q', timesyncdata)[0]
                else:
                    print("Time Sync frame read didn't return the proper number of bytes", file=sys.stderr)

            elif (byteRead == b'\xcf'):
                #we found our start byte.  Read another 5 bytes now
                framedata = file.read(5)
                if len(framedata) == 5:
                    unpackedFrame = struct.unpack('<2cHB', framedata)
                    #print(unpackedFrame)

                    frametimeoffset = int.from_bytes(unpackedFrame[0] + unpackedFrame[1], 'little')
                    #convert the frametimeoffset  from ms to us
                    frametimeoffset = frametimeoffset * 1000

                    frameid = unpackedFrame[2]

                    framelength = unpackedFrame[3] & 0x0f
                    busid = (unpackedFrame[3] & 0xf0) >> 4
                    if (framelength < 0):
                        framelength = 0
                    elif (framelength > 8):
                        framelength = 8

                    framepayload = file.read(framelength)
                    if frameid in DATA_DICT:
                        frametime = lastSyncTime + frametimeoffset
                        epoch_time = frametime / 1000000
                        date_time = datetime.datetime.fromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                        if epoch_time in epoch_dict:
                            parse_and_insert(frameid=frameid, payload=framepayload, epoch_time=epoch_time, epoch_dict=epoch_dict, convert_time=False)

                        elif min_epoch and epoch_time > min_epoch + MAX_SR :
                            csv_writer.writerow(epoch_dict[min_epoch])
                            del(epoch_dict[min_epoch])
                            epoch_dict[epoch_time] = ['NA'] * len(csv_filedsnames)
                            min_epoch = min(epoch_dict, key=epoch_dict.get)
                            parse_and_insert(frameid=frameid, payload=framepayload, epoch_time=epoch_time, epoch_dict=epoch_dict, convert_time=True)
                            
                        else:
                            epoch_dict[epoch_time] = ['NA'] * len(csv_filedsnames)  
                            parse_and_insert(frameid=frameid, payload=framepayload, epoch_time=epoch_time, epoch_dict=epoch_dict, convert_time=True)
                            if min_epoch == 0:
                                min_epoch = min(epoch_dict, key=epoch_dict.get)
                else:
                    break
        else:
            break
                
if (outputfile):
    outputfile.close()