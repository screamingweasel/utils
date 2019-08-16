#!/usr/bin/python
###################################################################################################
# Description: Merge Properties into a Ambari json format file
# Author: James Barnett
# History: 2019-01-20 Initial Release
###################################################################################################
import argparse
import sys
import json
#import tempfile
#import subprocess
#from subprocess import PIPE
from pprint import pprint
DEBUG=False

def parse_arguments():
	parser = argparse.ArgumentParser()
	parser.add_argument('configType')
	parser.add_argument('propertiesFileName')
	parser.add_argument('newPropertiesFileName')
	parser.add_argument('outputFileName')
	parser.add_argument('--debug', action='store_true', default=False)
	return parser.parse_args()

def read_properties(fileName):
	with open(fileName) as f:
		j = json.load(f)

	if "properties" not in j:
		sys.stderr.write("Key \"properties\" not found in json file\n")
		sys.exit(1)

	return j

def read_new_properties(fileName, configType):
	with open(fileName) as f:
		j = json.load(f)

	if "configurations" not in j:
		sys.stderr.write("Key configurations\" not found in new properties file\n")
		sys.exit(1)

	found=False
	config=None
	for c in j["configurations"]:
		if (configType in c):
			found=True
			config=c[configType]
			break

	if (found == False):
		sys.stderr.write("Configurations section \"%s\" not found in new properties file\n" % (configType))
		sys.exit(1)

	if "properties" not in config:
		sys.stderr.write("Properties section not found in \"%s\" configurations in new properties file\n" % (configType))
		sys.exit(1)

	return config["properties"]

###################################################################################################
def main():
	args=parse_arguments()
	DEBUG=args.debug
	print ("Updating %s with %s into %s") % (args.propertiesFileName, args.newPropertiesFileName, args.outputFileName)

	origJson = read_properties(args.propertiesFileName)
	props = origJson["properties"]
	newProps = read_new_properties(args.newPropertiesFileName, args.configType)

	keys=newProps.keys()
	for key in keys:
		value = newProps[key]
		if (value == '__DELETE__'):
			props.pop(key, None)
		else:
	 		props[key] = value

	if (DEBUG):
		print "\n----------------------  Original ----------------------"
		pprint(props)
		print "\n----------------------  Updates  ----------------------"
		pprint(newProps)
		print "\n----------------------  Updated  ----------------------"
		pprint(props)
		print ""


	outJson={"properties": props}
	with open(args.outputFileName,'w') as f:
         	f.write(json.dumps(outJson,ensure_ascii=False,indent=2))

	print "Updated properties written to %s" % (args.outputFileName)

##################################################################################################
# Main
##################################################################################################
if __name__ == "__main__":
    main()