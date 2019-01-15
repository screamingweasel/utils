#!/usr/bin/python
###################################################################################################
# Generic program to read files from AWS S3 containing text lines, merge and save in HDFS
# Will uncompress gzip files if needed, and optionally validate that each line contains valid json
# The lines of the file are combined and saved into HDFS into a configurable max number of files.
#
# The following steps are performed
#   1. Gather and validate parameters
#   2. Retrieve a set of keys from AWS S3 using the Boto library
#   3. Parallelize the keys into an RDD
#   4. FlatMap the RDD using the following logic:
#      A. Use AWS Boto to retrieve the entire contents of the key (file)
#      B. If the Keyname ends in .gz then uncompress it
#      C. Loop through each line of the file
#        i. If the parameter has been set then attempt to parse the line as json
#	 ii. Write the line out as the result of the FlatMap
#      D. Coalesce the result down to a specified number of partitions
#      E. SaveToTextFile to a specific, single HDFS directory
#
# Dependencies:
#   AWS Boto library
###################################################################################################
from boto.s3.connection import S3Connection
from fnmatch import fnmatch
from io import BytesIO
from os.path import expanduser
from pyspark import SparkContext, SparkConf
from zipfile import ZipFile
import boto
import json
import os
import sys
import time
import traceback
import zlib

###################################################################################################
# Gets AWS credentials. This looks for a json file "credentials.json" in the following locations:
#   1. Current working directory .
#   2. The conf directory under the current directory ./conf
#   3. ~/config/
#   4. ~/.aws
###################################################################################################
def get_aws_credentials(profile):
    CREDENTIALS_FILE = ""
   
    if (os.path.isfile(os.getcwd() + "/credentials.json")):
        CREDENTIALS_FILE = os.getcwd() + "/credentials.json"
    elif (os.path.isfile(expanduser("~") + "/config/credentials.json")):
        CREDENTIALS_FILE = expanduser("~") + "/config/credentials.json"
    else:
	print "Unable to find credentials.json in current directory or ~/config"
	sys.exit(1) 

    with open(CREDENTIALS_FILE) as data_file:
        data = json.load(data_file)
    	
    if (data[profile] == None):
        print "Cannot find profile " + profile + " in credentials file " + CREDENTIALS_FILE
        sys.exit(1)

    if (data[profile]["aws_access_key_id"] == None or data[profile]["aws_secret_access_key"] == None):
        print "Cannot find aws keys for profile " + profile + " in credentials file " + CREDENTIALS_FILE
        sys.exit(1)

    return {"aws_access_key_id": data[profile]["aws_access_key_id"], "aws_secret_access_key": data[profile]["aws_secret_access_key"]}

###################################################################################################
# Get a boto.s3.bucketlistresultset collection from a bucket with keys matching a prefix
###################################################################################################
def get_aws_files(awsCredentials, bucketName, prefix, printFiles=False, endPoint="", signature="v4"):
	if (signature == "v4"):
		boto.config.add_section('s3')
		boto.config.set('s3', 'use-sigv4', 'True')

	conn = S3Connection(awsCredentials["aws_access_key_id"], awsCredentials["aws_secret_access_key"],host=endPoint)
	buck = conn.get_bucket(bucketName)
	files = buck.list(prefix=prefix)
	if (printFiles == True):
		i=0
		print "\n" + '*' * 100
		print "Files in bucket %s matching prefix %s" % (bucketName,prefix)
		print "" + '*' * 100
		for file in files:
			i=i+1
			print file.name
		print "\nTotal files: %s\n" % (i)
	return files

###################################################################################################
# Search command line arguments for an optional parameter in the form --xxx and returns the value
# of the next item in the argument list. If not found returns default.
###################################################################################################
def get_optional_parm(args, parmName, default):
	value = default
	if (parmName in args):
		i = args.index(parmName)
		if (len(args) > i+1):
			value = str(args[i+1])

	return value

###################################################################################################
# Spark map function to pull file from AWS, unzip, and return as individual lines.
###################################################################################################
def map_func(key):
	global lineCnt
	global errorCnt
	global byteCnt
	global bytesOutCnt
	global compressedByteCnt
	global validateJson

	contents = u''
	uncompressed = u''

	try:
		contents = key.get_contents_as_string()
		compressedByteCnt += len(contents)
		# if it looks like a gzip file then uncompress
		if (key.name.encode('utf-8')[-3:] == u'.gz'):
			uncompressed = zlib.decompress(contents, 16+zlib.MAX_WBITS)
		# if it looks like a zip file then uncompress and concatenate all files in the zip
                elif (key.name.encode('utf-8')[-4:] == u'.zip'):
			zipContents = []
			zipfile = ZipFile(BytesIO(contents))
			for zfile in zipfile.namelist():
				zipContents.append(zipfile.open(zfile).read())
			uncompressed = ''.join(zipContents)
		else:
			uncompressed = contents
			
		byteCnt += len(uncompressed)
	except Exception as ex:
		print "+" * 100
		print "++++ An error getting contents for S3 key %s" % (key.name)
		print type(ex)
		print ex.args
		print (ex)
		traceback.print_exc()
		print "+" * 100

	for line in uncompressed.splitlines():     
		bytesOutCnt += len(line)
		lineCnt +=1
		try:
			# Attempt to parse json if flag was set
			if (validateJson == 1):
				json_object = json.loads(line)

			bytesOutCnt += len(line)
			yield line
		except ValueError, e:
			errorCnt +=1
			print "Invalid json found in map_func for key %s" % (key.name)
			traceback.print_exc()

###################################################################################################
# Main
###################################################################################################
if __name__ == "__main__":
	if (len(sys.argv) < 7):
		print 'Incorrect number of parameters'
		print 'Usage: ' + str(sys.argv[0]) + ' <aws profile> <bucket> <prefix> <destination> <endpoint> <signature> '
		print '[--pattern *] [--json true|FALSE] [--loglevel WARN|debug|info] [--partitions n] [--slices n] [--compression <codec>]'
		sys.exit(1)

	startTime = time.time()

	###########################################################################################
	# Get parameters
	###########################################################################################
	# Positional, required arguments
	profile = str(sys.argv[1])
	bucket = str(sys.argv[2])
	prefix = str(sys.argv[3])
	destination = str(sys.argv[4])
	endpoint = str(sys.argv[5])
	signature = str(sys.argv[6])

	# Optional named arguments
	validateJson = 1 if (get_optional_parm(sys.argv[1:], "--json", "false").lower() =="true") else 0
	logLevel = get_optional_parm(sys.argv[1:], "--loglevel", "WARN").upper()
	maxPartitions = int(get_optional_parm(sys.argv[1:], "--partitions", "32"))
	slices = int(get_optional_parm(sys.argv[1:], "--slices", "32"))
	codec = get_optional_parm(sys.argv[1:], "--compression", "org.apache.hadoop.io.compress.SnappyCodec")
	pattern = get_optional_parm(sys.argv[1:], "--pattern", "*")

	print "\nmaxPartitions=%s" % (maxPartitions)
	print "profile=%s" % (profile)
	print "bucket=%s" % (bucket)
	print "prefix=%s" % (prefix)
	print "pattern=%s" % (pattern)
	print "endpoint=%s" % (endpoint)
	print "signature=%s" % (signature)
	print "destination=%s" % (destination)
	print "maxPartitions=%s" % (maxPartitions)
	print "slices=%s" % (slices)
	print "validateJson=%s" % (validateJson)
	print "logLevel=%s" % (logLevel)
	print "codec=%s\n" % (codec)

	###########################################################################################
	# Setup the Spark context
	###########################################################################################
	awsCredentials =  get_aws_credentials(profile)
	conf = SparkConf().setAppName(str(sys.argv[0]))
	sc = SparkContext(conf=conf)
	sc.setLogLevel(logLevel)
	sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", awsCredentials["aws_access_key_id"])
	sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", awsCredentials["aws_secret_access_key"])

	# Setup Spark accumulators for totals
	lineCnt=sc.accumulator(0)
	errorCnt=sc.accumulator(1)
	errorCnt = 0
	compressedByteCnt=sc.accumulator(2)
	byteCnt=sc.accumulator(3)
	bytesOutCnt=sc.accumulator(4)
	validateJson=sc.accumulator(5)

	###########################################################################################
	# Get the AWS keys, parallellize them into an RDD, and filter based on the pattern
	###########################################################################################
	keys = get_aws_files(awsCredentials, bucket, prefix, printFiles=True, endPoint=endpoint, signature=signature)
	pkeys = sc.parallelize(keys, slices).filter(lambda key: fnmatch(key.name.encode('utf-8'), pattern))

	# Print the filtered keys
	collectedKeys = pkeys.collect()
	print "\n" + '*' * 100
	print "Keys filtered by %s" % (pattern)
	print '*' * 100
	for k in collectedKeys:
		print k.name.encode('utf-8')
	print "\n"

	# This is where the magic happens!
	pkeys.flatMap(map_func).coalesce(maxPartitions).saveAsTextFile(destination,compressionCodecClass=codec)

	elapsedTime = time.time() - startTime

	print "\n" + '*' * 100
	print "Output Directory = %s" % (destination)
	print "Lines Processed = %s" % (lineCnt)
	print "Lines in error  = %s" % (errorCnt)
	print "Compressed Bytes read = %s" % (compressedByteCnt)
	print "Uncompressed Bytes read = %s" % (byteCnt)
	print "Bytes Written = %s" % (bytesOutCnt)
	print "Elapsed Time = %d seconds" % (elapsedTime)
	print '*' * 100
