#!/usr/bin/python
###################################################################################################
# Description: update a blueprint file from an update json document
# Author: James Barnett
# History:
###################################################################################################
import argparse
import sys
import json
from pprint import pprint

###################################################################################################
# Read a blueprint file into a dictionary and perform basic error checking
###################################################################################################
def read_blueprint(fileName):
	with open(fileName) as f:
		j = json.load(f)

	if "configurations" not in j:
		print "Key \"configurations\" not found in json file. Invalid blueprint format"
		sys.exit(1)

	return j

###################################################################################################
# Read all the updates into an easy to read dictionary
###################################################################################################
def read_all_updates(fileName):
	with open(fileName) as f:
		j = json.load(f)

	if "configurations" not in j:
		sys.stderr.write("Key configurations\" not found in new properties file\n")
		sys.exit(1)

	updates = {}
	for c in j["configurations"]:
		keys = c.keys()
		for key in keys:
			if (key not in updates):
				updates[key] = {}
			if ("properties" in c[key]):
				properties = c[key]["properties"]
				pkeys = properties.keys()
				for pkey in pkeys:
					updates[key][pkey] = properties[pkey]

	return updates

###################################################################################################
# Apply the updates to the blueprint
###################################################################################################
def apply_updates(blueprint, updates):

	# The key is the config section
	uKeys = updates.keys()
	for uKey in uKeys:
		update = updates[uKey]
		# These are the properties
		pKeys = update.keys()
		for pKey in pKeys:
			#print uKey, pKey, update[pKey]
			
			##### THIS IS NOT WORKING! #####
			if (uKey in blueprint["configurations"][0]):
				print "Found %s, updating %s" %(uKey, pKey)
				blueprint["configurations"][0][pKey] = update[pKey]
			else:
				print "Did not find %s, ignoring %s" %(uKey, pKey)
		
###################################################################################################
# Main
###################################################################################################
def main():
	if (len(sys.argv) != 4):
		print 'Incorrect number of parameters\nUsage: %s <blueprint> <updates> <updated blueprint>' % (str(sys.argv[0]))
		sys.exit(1)

	blueprint = read_blueprint(sys.argv[1])
	updates = read_all_updates(sys.argv[2])
	newBlueprint = apply_updates(blueprint, updates)
	
	with open(sys.argv[3],'w') as f:
		f.write(json.dumps(newBlueprint,ensure_ascii=False,indent=2))

if __name__ == "__main__":
    main()