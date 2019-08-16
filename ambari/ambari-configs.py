#!/usr/bin/python
###################################################################################################
# Description: Helper script to perform functions of ambari configs.py removed in HDP 2.6
#              Reads a json file of updated properties and applies them in one batch to Ambari
# TODO: Parameterize and add error handling
# Author: James Barnett
# History:
###################################################################################################
import sys
import json
import tempfile
import subprocess
from subprocess import PIPE
from pprint import pprint
DEBUG=True

###################################################################################################
# execute an os command and return results
###################################################################################################
def os_exec(command):
    import subprocess
    from subprocess import PIPE
    
    if (DEBUG):
    	print "Executing\n%s" % command
    pobj = subprocess.Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
    stdo, stde = pobj.communicate()
    exit_code = pobj.returncode

    return exit_code, stdo, stde
    
###################################################################################################n
# Main 
###################################################################################################
def main():
	if (len(sys.argv) < 3):
		print 'Incorrect number of parameters\nUsage: ' + str(sys.argv[0]) + ' <config-type> <new propertiesi file>'
		sys.exit(1)

	user="admin"
	password="cursor11"
	port="8080"
	host="csc2cxp00001336.cloud.kp.org"
	cluster="kpadfdmprod01"
	configType=sys.argv[1]
	newPropertiesFile=sys.argv[2]

	CONFIG_CMD="/var/lib/ambari-server/resources/scripts/configs.py --user=%s --password=%s --port=%s --action=%s --host=%s --cluster=%s --config-type=%s --file=%s"
	
	tmpFileName=tempfile._get_default_tempdir() + "/" + next(tempfile._get_candidate_names()) + ".json"
	command = CONFIG_CMD % (user, password, port, "get", host, cluster, configType, tmpFileName)

	exit_code, stdo, stde = os_exec(command)
	if exit_code != 0:
		print "Unable to read configs, exit_code=%s" % (exit_code)
		sys.exit(exit_code)

	# Read the json output		
        with open(tmpFileName) as f:
                j=json.load(f)
	pprint(j)

	# Do any updates / inserts
	props=j["properties"]
	if (DEBUG):
		print "-------- OLD JSON --------"
		pprint(props)

        with open(newPropertiesFile) as f:
                newProps=json.load(f)["properties"]
	if (DEBUG):
    		print "-------- NEW PROPS --------"
		pprint(newProps)
	
	keys=newProps.keys()
	for key in keys:
		value=newProps[key]
		props[key] = value
		if (DEBUG):
			print "Setting [%s] to %s" % (key,value)

	# Write out the updated json
	with open(tmpFileName,'w') as f:
        	f.write(json.dumps(j,ensure_ascii=False,indent=2))
	#write_json_file(tmpFileName, j)
	if (DEBUG):
		print "-------- NEW JSON --------"
        	pprint(j)
	
	print "Updated json written to %s" % (tmpFileName)

	command = CONFIG_CMD % (user, password, port, "set", host, cluster, configType, tmpFileName)
        exit_code, stdo, stde = os_exec(command)
	if exit_code != 0:
		print "Unable to update configs, exit_code=%s" % (exit_code)
		sys.exit(exit_code)

	print "Update to Ambari complete"	

##################################################################################################
# Main
##################################################################################################
if __name__ == "__main__":
    main()