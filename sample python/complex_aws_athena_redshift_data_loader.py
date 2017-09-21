import pandas as pd
import json
import csv
from os.path import basename
import os
import gzip
import boto3
import boto
import boto.s3
import os.path
import sys
from sqlalchemy import create_engine
import pandas as padas

###json keys must be lower case as expressed below for redshift to see them athena doesn't care


##### NOTE: No logic in .py that asks is this file present on s3 or in directories probably overwrites files #####
### add logic for not exists on s3 AND sftp (local) so files dont have to be rerun
#run all of this for client in list of clients #listener on sftp
#sftp location = x
#output_directory for zip = s3
# Fill these in - you get them when you sign up for S3


input_directory =  ''#string
output_directory = ''#string

AWS_ACCESS_KEY_ID = ''#string
AWS_ACCESS_KEY_SECRET = ''#string
iamROLE = iam_role ''#string
redshift_connection = ''#string


s3_bucket = ''#string
s3_location= ''#string

organization_code = 'org1' #passed from sftp name
s3_outputdirectory = s3_location.replace("s3://"+s3_bucket,"")

athena_database = 'spectrumdb'
redshift_database = 'public'

###create logic to determine if adhoc_data or warehouse_data
### create a function that assigns based on file name better s3 directories
s3_outputdirectory = s3_outputdirectory+organization_code+"/"
adhoc_data_file_s3_outputdirectory = s3_outputdirectory+"data/adhoc_data/"
warehouse_data_file_s3_outputdirectory = s3_outputdirectory+"data/warehouse_data/"

ddl_file_s3_outputdirectory =s3_outputdirectory+"ddl_files/"

warehouse_data_types = [] #
warehouse_data_types.append('iidw_')#import warehouse_data_types_unlimited via match in determine adhoclogic
#manage data_types in database via ids


###support different warehouse formats via print here#####
def s3_determine_adhoc(input_file,adhoc_data_file_s3_outputdirectory,warehouse_data_file_s3_outputdirectory):
    input_file = os.path.splitext(basename(input_file))[0]
    is_adhoc = ""
    if "fiel_trigger" not in input_file:
        is_adhoc = adhoc_data_file_s3_outputdirectory
    else:
        is_adhoc = warehouse_data_file_s3_outputdirectory
    return is_adhoc

def local_determine_adhoc(input_file):
    if "iidw_" not in input_file:
        is_adhoc = "adhoc_data/"
    else:
        is_adhoc = "warehouse_data/"
    return is_adhoc


#create folders if not exist logic
json_output_directory = output_directory+ "json_files/"
ddl_outdirectory = output_directory+"ddl_files/"


#boto3 test
#s3 = boto3.resource('s3')
#for bucket in s3.buckets.all():
#    print(bucket.name)
#unused
##def s3_uploader(source_path_filename,s3_bucket,s3_path):
## key = s3_path#+basename(source_path_filename)
##  print "####"+key
##  s3.meta.client.upload_file(source_path_filename, s3_bucket, key)


## run .xlsx logic first to convert to csv ##
## auto create
####add logic for not exists on s3 AND sftp (local) so files dont have to be rerun
def get_files(input_directory):
    dir_list = []
    for file in os.listdir(input_directory):
        if file.endswith(".csv") :
            path = os.path.join(input_directory, file)
            dir_list.append(path)


    return dir_list
#create logic to not run if already in directory
#save this as a key value pair instead ie path:: name

def get_ath_headers(input_file):
    file_name = basename(input_file.replace(".csv",""))
    print "----------"+ input_file
    data = pd.read_csv(input_file, nrows=0, error_bad_lines=False)
    headers = data.columns.values.tolist()
    headers = str(headers)
    headers = headers.replace(","," string,\n")
    # replace_null = ["[","]","'"] # list to pass for many replaces
    headers = headers.replace("'",'`') #` char for ddl column quotes
    headers = headers.replace("[","")
    headers = headers.replace ("]"," string")
    headers = headers.replace('"',"")
    headers = headers.replace('',"")

    return headers

def create_athena_ddl(s3_location,athena_database,input_file,output_directory, headers):

    file_name = basename(input_file.replace(".csv",""))
    file_name = basename(file_name.replace(".json",""))
    file_name =  file_name.replace(" ","")
    file_name =  file_name.replace("_","")
    table_name = file_name
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


def get_redshift_headers(input_file):
    data = pd.read_csv(input_file, nrows=0, error_bad_lines=False)
    headers = data.columns.values.tolist()
    headers = str(headers)
    headers = headers.replace(","," varchar(255),\n")

    headers = headers.replace("[","(")
    headers = headers.replace ("]"," varchar(255))")
    headers = headers.replace('"',"")
    headers = headers.replace("'","")

    return headers

def create_redshift_ddl(s3_location,redshift_database,input_file,output_directory, headers):
    file_name_lit = basename(input_file)

    file_name = basename(input_file.replace(".gz",""))
    file_name = basename(input_file.replace(".csv",""))
    file_name = basename(file_name.replace(".json",""))
    file_name =  file_name.replace(" ","")
    file_name =  file_name.replace("_","")
    table_name = file_name
    full_file_name = output_directory+file_name+"_rs_ddl.txt"
    file_name_lit = file_name_lit.replace(".csv",".json.gz")
    #for row in table create go to next line wayyyyyy to long at 100 header tables
    text_file = open(full_file_name, "w")

    if table_name[0].isdigit():
        table_name = "table_prefix"+table_name
    else:
        table_name

    ii_ddl = "create table if not exists "+redshift_database+"."+ table_name+"\n"+headers+";" \
             +"\n"+"\ncopy "+redshift_database+"."+table_name+" from "+"'"+s3_location+file_name_lit+"'"+ \
             "\n%s \njson 'auto' gzip ;"  % iamROLE

    #print ii_ddl
    text_file.write(ii_ddl)

    text_file.close()

    return full_file_name



def csv_to_json(input_file, output_directory):
    in_format = ".csv"
    out_format = '.json'
    #print "#####"+output_directory
    csvfile = open(input_file, 'r')
    file_name = basename(input_file.replace(in_format,""))

    jsonfile = open(output_directory+file_name+out_format, 'w')
    reader = csv.DictReader(csvfile)

    for row in reader:
        #print row
        row = dict((k.lower(), v) for k,v in row.iteritems())
        #print row
        json.dump(row, jsonfile)
        jsonfile.write('\n')
    out_file = output_directory+file_name+out_format

    return out_file

#s3 = outputdirectory
def gzip_jsons(input_file,localout,output_directory): #change this output dirctory
    in_format = '.json'
    out_format = in_format +'.gz'
    zipped_outdirectory = "gzip_files/"
    file_name = basename(input_file.replace(in_format,""))
    s3_input_dir = output_directory+localout+zipped_outdirectory
    output_dir_zipped = output_directory+localout+zipped_outdirectory+file_name+out_format
    with open(input_file,'r') as f_in, gzip.open(output_dir_zipped, 'wb') as f_out:
        f_out.writelines(f_in)


        return s3_input_dir


def s3_batch_loader(AWS_ACCESS_KEY_ID,AWS_ACCESS_KEY_SECRET,bucket_name,sourceDir,destDir):

    #max size in bytes before uploading in parts. between 1 and 5 GB recommended
    MAX_SIZE = 20 * 1000 * 1000
    #size of parts when uploading in parts
    PART_SIZE = 6 * 1000 * 1000

    conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET)

    bucket = conn.create_bucket(bucket_name,
                                location=boto.s3.connection.Location.DEFAULT)


    uploadFileNames = []
    for (sourceDir, dirname, filename) in os.walk(sourceDir):
        uploadFileNames.extend(filename)
        break

    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()

    for filename in uploadFileNames:
        sourcepath = os.path.join(sourceDir + filename)
        destpath = os.path.join(destDir, filename)
        print 'Uploading %s to Amazon S3 bucket %s' % \
              (sourcepath, bucket_name)

        filesize = os.path.getsize(sourcepath)
        if filesize > MAX_SIZE:
            print "multipart upload"
            mp = bucket.initiate_multipart_upload(destpath)
            fp = open(sourcepath,'rb')
            fp_num = 0
            while (fp.tell() < filesize):
                fp_num += 1
                print "uploading part %i" %fp_num
                mp.upload_part_from_file(fp, fp_num, cb=percent_cb, num_cb=10, size=PART_SIZE)

            mp.complete_upload()

        else:
            print "singlepart upload"
            k = boto.s3.key.Key(bucket)
            k.key = destpath
            k.set_contents_from_filename(sourcepath,
                                         cb=percent_cb, num_cb=10)


            ######## Query Via Spectrum By Creating Athena Tables######
            ######## begin athena table creation ########




            ######## Upload to Redshift ######
            ######## begin redshift table creation ########




###################################function calls########################
#def user_input:


input_directory = get_files(input_directory)



def zip_em(input_directory):
    for input_file in input_directory:
        print input_file
        #s3outdirectory = s3_determine_adhoc(input_file,adhoc_data_file_s3_outputdirectory,warehouse_data_file_s3_outputdirectory)
        #ath_header = get_ath_headers(input_file)
        #ddl_s3_dir = "s3://"+s3_bucket+s3outdirectory
        #athena_ddl_out = create_athena_ddl(ddl_s3_dir,athena_database,input_file,ddl_outdirectory,ath_header)
        #rs_header = get_redshift_headers(input_file)
        #rs_ddl_out = create_redshift_ddl(ddl_s3_dir,redshift_database,input_file,ddl_outdirectory,rs_header)

        #localout = local_determine_adhoc(input_file)
        #json_file = csv_to_json(input_file, json_output_directory)
        #zip_out = gzip_jsons(json_file,localout,output_directory)
        #rs_header = get_redshift_headers(input_file)
        #rs_ddl_out = create_redshift_ddl(ddl_s3_dir,redshift_database,input_file,ddl_outdirectory,rs_header)


        #s3_batch_loader(AWS_ACCESS_KEY_ID,AWS_ACCESS_KEY_SECRET,s3_bucket,zip_out,s3outdirectory)




zip_em(input_directory)
#s3_batch_loader(AWS_ACCESS_KEY_ID,AWS_ACCESS_KEY_SECRET,s3_bucket,ddl_outdirectory,ddl_file_s3_outputdirectory)
print ddl_outdirectory

#if ware house no need to run create table ddl in athena
#   else: run ddl in athena

#os.path.splitext('/home/user/somefile.txt')[0]
#load file to s3
#run ddl in athena
#based on file type pull in athena connection or spectrum connection to test data
#based on file type ctas over to redshift
#notify of file aquision


def run_rs_ddls(input_directory):
    directory = input_directory
    for file in os.listdir(input_directory):
        table_name = file
        if table_name[0].isdigit():
            table_name = "ii"+table_name
        else:
            table_name
        table_name = table_name.replace("_rs_ddl.txt","")

        if file.endswith("_rs_ddl.txt") :

            with open(directory+file, 'r') as myfile:
                data = str(myfile.read().replace('\n', ''))
                engine = create_engine(redshift_connection)
                data_frame = padas.read_sql_query('select count(*) from public.'+table_name, engine)
                print "Count of "+table_name+ "before"+str(data_frame)
                engine.execute (data)
                data_frame = padas.read_sql_query('select count(*) from public.'+table_name, engine)
                print "Count of "+table_name+ "after"+str(data_frame)
    print \
        "--------COMPLETE--------"





run_rs_ddls(ddl_outdirectory)