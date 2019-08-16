###############################################################################
# Jython program to dump a CSV file
###############################################################################
import csv
import sys
import json
from pprint import pprint

inputFileName = '//Users/jamesbarnett/t/newrelic.txt'
inFile = open(inputFileName, "r")

line = inFile.readline()
while (line != "") :
	j = json.loads(line)
	for key in j.keys():
		print (key)
	line = inFile.readline()

inFile.close()

