#!/usr/bin/python
###################################################################################################
# Description: Take Tab separated output from Blueprint Compare and create and update json doc
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

###################################################################################################
# Parse tsv input
###################################################################################################			
def parse_line(line):
	values = line.split("\t")

	if (len(values) != 6):
		sys.stderr.write(str(len(values)) + " columns found, expected 6\n" + line + "\n")
		sys.exit(1)

	data = {
		"configType": values[0],
		"key": values[1],
		"status": values[2],
		"value": values[3],
		"newValue": values[4]}
	
	return data
				
###################################################################################################
# Main
###################################################################################################
def main():

	configType = ""
	updates = {"configurations": []}
	line = sys.stdin.readline()
	while line:
		data = parse_line(line)
		
		if (configType != data["configType"]):
			print "==== New config Type %s ====" % (data["configType"])
			configType = data["configType"]
			while (line && configType = data["configType"]):
				print data["configType"], data["key"], data["status"]	
				line = sys.stdin.readline()
				if (line):
					data = parse_line(line)
			
			print "====== Completed configType ======"
			
			
			
##################################################################################################
# Main
##################################################################################################
if __name__ == "__main__":
    main()