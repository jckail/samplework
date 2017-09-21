import pandas as pd
from pandas import compat
import numpy
import json
import os
from os.path import basename
from os import stat
from pwd import getpwuid
import os
import os.path, time
import platform

s3_location= ''
athena_database = ''
client = 'testorg'
permissions = 'all'
input_directory = ''


def creation_date(path_to_file):
    return time.ctime(os.path.getctime(path_to_file))

def modify_date(path_to_file):
    return time.ctime(os.path.getmtime(path_to_file))

def find_owner(filename):
    return getpwuid(stat(filename).st_uid).pw_name


master_table = pd.DataFrame(None)

def get_files(input_directory):
    dir_list = []
    for file in os.listdir(input_directory):
        if file.endswith(".csv") :
            path = os.path.join(input_directory, file)
            dir_list.append(path)
    return dir_list


def file_data_frame_gen (file): #,file_count
    frame = pd.read_csv(file)

    frame['filetype'] = 'to_be_assigned'
    #frame['file_batch_no'] = file_count.replace("-","")
    frame['filename'] = os.path.basename(file)
    frame['user'] = find_owner(file)
    frame['createtime'] = creation_date(file)
    frame['modifytime'] = modify_date(file)

    return frame

def frame_to_json (frame_name,client,permissions):

    out = frame_name.apply(lambda x: list(x.dropna()), axis=1).to_json(orient='records')[1:-1].replace('},{', '} {')
    out = out.replace("}","}\n")
    with open(input_directory+client+'.json', 'w') as f:
        f.write(out)
    output = input_directory+'master_table.json'
    return output

def frame_to_csv(frame_name):
    frame_name.to_csv(input_directory+'master_table.csv',encoding='utf-8', index = False)

def rename_dupe_headers(master_table):
    col_list = list(master_table.columns)
    col_out = []
    #print "----"+str(col_list)
    for col in col_list:
        if col_list.count(col) > 1:
            col = col + 'a'
            col_out.append(col)
        else:
            col = col
            col_out.append(col)
    master_table.columns = col_out
    a = master_table['file_batch_no'].head(5)
    print a

def create_athena_ddl(s3_location,athena_database,input_file,output_directory, headers, client):

    file_name = basename(input_file.replace(".csv",""))
    file_name = basename(file_name.replace(".json",""))
    file_name =  file_name.replace(" ","")
    file_name =  file_name.replace("_","")
    table_name = file_name
    headers = str(headers)
    headers = headers.replace(","," string,\n")
    # replace_null = ["[","]","'"] # list to pass for many replaces
    headers = headers.replace("'",'`') #` char for ddl column quotes
    headers = headers.replace("[","")
    headers = headers.replace ("]"," string")
    headers = headers.replace('"',"")
    headers = headers.replace('',"")

    if table_name[0].isdigit():
        table_name = "ii"+table_name
    else:
        table_name

    full_file_name = output_directory+file_name+"_athena_ddl.txt"
    text_file = open(full_file_name, "w")

    ii_ddl = "CREATE EXTERNAL TABLE IF NOT EXISTS " \
             +athena_database +"."+table_name \
             +"\n ("+headers+")"+"\n ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'WITH SERDEPROPERTIES ('serialization.format' = '1') \
             \nLOCATION '" \
             +s3_location+"'\nTBLPROPERTIES ('has_encrypted_data'='false')"

    text_file.write(ii_ddl)

    text_file.close()

    return full_file_name

def main():
    dir_list = get_files(input_directory)
    master_table = pd.DataFrame(None)
    #file_count = 0
    for file in dir_list:

        file_df = file_data_frame_gen(file) #,"-"+str(file_count)
        #master_table = pd.concat([file_df,master_table], axis = 1)
        master_table = master_table.append(file_df,ignore_index = True)
    master_table = master_table.drop_duplicates()
    json_file = frame_to_json(master_table,client,permissions)
    headers = list(master_table.columns)


main()