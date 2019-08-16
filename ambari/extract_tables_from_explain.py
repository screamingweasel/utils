###############################################################################
# Python script to parse output of hive explain plan and extract tables
###############################################################################
import collections
import csv
import sys
import json
from pprint import pprint
tag=""

###################################################################################################
# 
###################################################################################################
def hobble(d, tag):
	#print "\n----------------------- %s -----------------------" % (level)
	if (isinstance(d, dict)):
		#print "----> d is a dict"
		#pprint(d)
		keys = d.keys()
		if (u'database:' in d and u'table:' in d):
			print "%s\t%s.%s" % (tag, d['database:'], d['table:'])
		if (len(keys) > 1):
			for key in keys:
				#print "recursively calling hobble for key %s" % (key)
				hobble({key: d[key]}, tag)
		else:
			value = d[keys[0]]
			if (not isinstance(value, list) and not isinstance(value, dict)):
				foo="bar"
				#print "====> yahoo, got a root value!"
				#print "%s=%s" % (keys[0],value)
			else:
				hobble(value, tag)
	elif (isinstance(d, list)):
		#print "----> d is a list"
		#pprint(d)
		for v in d:
			hobble(v, tag)
	else:
		foo="bar"
		#print "###Type {} not recognized".format(type(d))
		#pprint(d)

###################################################################################################
# 
###################################################################################################
def main():

	if (len(sys.argv) < 3):
		print 'Incorrect number of parameters'
		print 'Usage: ' + str(sys.argv[0]) + ' <json file> <key>'
		sys.exit(1)

	inputFileName = str(sys.argv[1])
	tag = str(sys.argv[2])
	
	with open(inputFileName) as json_file:
		data = json.load(json_file)

	hobble(data, tag)
  
if __name__ == "__main__":
        main()
