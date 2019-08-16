#!/usr/bin/python
###################################################################################################
# Description: Print out all the configs from a blueprint in a tsv format
# Author: James Barnett
# History:
###################################################################################################
import argparse
import sys
import json
from pprint import pprint

A_NOT_B = "A not B"
B_NOT_A = "B not A"
MATCHED = "Matched"
CHANGED = "Changed"
OPER_DELETE = "__DELETE__"

###################################################################################################
# Parse input arguments using argparse
###################################################################################################
def parse_arguments():
	parser = argparse.ArgumentParser()
	parser.add_argument('blueprintFile1')
	parser.add_argument('--debug', action='store_true', default=False)
	return parser.parse_args()

###################################################################################################
# Read blueprint file and perform basic validity checking
###################################################################################################
def read_properties(fileName):
	with open(fileName) as f:
		j = json.load(f)

	if "configurations" not in j:
		print "Key \"configurations\" not found in json file. Invalid blueprint format"
		sys.exit(1)

	return j

###################################################################################################
# Parse the blueprint file and return a flattened list of configTypes and Keys
###################################################################################################
def parse_configs(configs):
	configList = []
	for config in configs:
		configType = config.keys()[0]
		#print "configType=%s'" % configType
		if ("properties" in config[configType]):
			props = config[configType]["properties"]
			propKeys = props.keys()
			for pk in propKeys:
				val = str(props[str(pk)])
				#print "  %s=%s" % (str(pk), val)
				compositeKey = configType + "." + str(pk)
				configList.append({"compositeKey": compositeKey, "configType": configType, "key": str(pk), "value": val})

	return configList

###################################################################################################
# Utility to cleanup multi-line config values
###################################################################################################	
def clean_value(value):
	return str.replace(str.replace(value,"\n", "\\n"),"\t","\\t")
	
###################################################################################################
# Do the compare
###################################################################################################
def compare_configs(configs1, configs2, showOnlyDiffs):
	diffs = []
	
	# Old not new and matched
	for entry1 in configs1:
		value1 = clean_value(str(entry1["value"]))
		entry1["value"] = value1
		
		# Found in both places
		#print ("Searching for %s") % (entry1['compositeKey'])
		entry2 = filter(lambda conf: conf['compositeKey'] == entry1['compositeKey'], configs2)
		#pprint(entry2)

		#print "==== Entry 1 ===="
		#pprint(entry1)
		#print "==== Entry 2 ===="
		#pprint(entry2)
		#print "#############################################################################"
		if (len(entry2) > 0):
			value2 = clean_value(str(entry2[0]["value"]))
			#print value1
			#print value2
			#print "#############################################################################"

			# Not filter returns a list of dicts
			if (value1 != value2):
				entry1["newValue"] = value2
				entry1["status"] = CHANGED
				#print CHANGED + " " + entry1['compositeKey']
				diffs.append(entry1)
			elif (showOnlyDiffs == False):
				entry1["newValue"] = value2
				entry1["status"] = MATCHED
				diffs.append(entry1)
				#print MATCHED + " " + entry1['compositeKey']
		# Old Not New
		else:
			entry1["newValue"] = ""
			entry1["status"] = A_NOT_B
			diffs.append(entry1)
			#print A_NOT_B  + " " + entry1['compositeKey']

	# New not Old			
	for entry in configs2:
		entry1 = filter(lambda conf: conf['compositeKey'] == entry['compositeKey'], configs1)
		if (len(entry1) == 0):
			entry["newValue"] = clean_value(entry["value"])
			entry["value"] = ""
			entry["status"] = B_NOT_A
			diffs.append(entry)
			#print B_NOT_A + " " + entry['compositeKey']
			
	return diffs
					
###################################################################################################
# Print Diffs
###################################################################################################
def print_diffs(diffs, format):
	if (format == "csv"):
		print "configType\tKey\tStatus\tValueA\tValueB"
		for diff in diffs:
			print "%s\t%s\t%s\t%s\t%s" % (diff["configType"],diff["key"],diff["status"],diff["value"],diff["newValue"])
			
###################################################################################################
# Write updates in json format for consumption by merge_properties.py
###################################################################################################
def write_updates(diffs, updateFile):
	# Need to sort by configType to generate control breaks
	diffsSorted = sorted(diffs, key = lambda i: (i['configType'], i['key'])) 
	configType = ""
	updates = {"configurations": []}
	config = None
	props = None
	for diff in diffsSorted:
		# Handle control break
		if (configType != diff["configType"]):
			if (configType != ""): # Don't add config for initial loop 0
				updates["configurations"].append(config)

			configType = diff["configType"]
			config = {configType: {"properties": {}}}
			props = config[configType]["properties"]		
		
		status = diff["status"]
		if (status == B_NOT_A or status == CHANGED):
			props[diff["key"]] = diff["newValue"]
		elif (status == A_NOT_B):
			props[diff["key"]] = OPER_DELETE

	# If any diffs in loop then add the last config section		
	if (configType != ""):
		updates["configurations"].append(config)

	with open(updateFile, 'w') as f:
		f.write(json.dumps(updates, indent=2))

###################################################################################################
# Main
###################################################################################################
def main():
	args=parse_arguments()
	pprint(args)
	blueprintFile1=args.blueprintFile1

	msg = "Parsing %s\n" % (blueprintFile1)
	sys.stderr.write(msg)

	blueprint1 = read_properties(blueprintFile1)
	configs1 = parse_configs(blueprint1["configurations"])
	for c in configs1:
		value = c["value"].replace("\n","\\n")
		print "%s\t%s\t%s\t%s" % (c["compositeKey"],c["configType"],c["key"],value)
			
if __name__ == "__main__":
    main()