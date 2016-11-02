#!/usr/bin/env python

"""
terra.meteorological.py

This extractor will trigger when a file is added to a dataset in Clowder.
It checks if all the required input files are present in the dataset and no metadata
indicating the dataset has already been processed.
If the check is OK, it aggregates all input files into one JSON and inserts it into
GeoStream.
"""

import os
import csv
import json
import requests
import urllib
import urlparse
import logging
from config import *
from parser import *
import pyclowder.extractors as extractors


def main():
	global extractorName, messageType, rabbitmqExchange, rabbitmqURL, registrationEndpoints, mountedPaths

	#set logging
	logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
	logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)
	logger = logging.getLogger('extractor')
	logger.setLevel(logging.DEBUG)

	# setup
	extractors.setup(extractorName=extractorName,
					 messageType=messageType,
					 rabbitmqURL=rabbitmqURL,
					 rabbitmqExchange=rabbitmqExchange,
					 mountedPaths=mountedPaths)

	# register extractor info
	extractors.register_extractor(registrationEndpoints)

	#connect to rabbitmq
	extractors.connect_message_bus(extractorName=extractorName,
								   messageType=messageType,
								   processFileFunction=process_dataset,
								   checkMessageFunction=check_message,
								   rabbitmqExchange=rabbitmqExchange,
								   rabbitmqURL=rabbitmqURL)

def check_message(parameters):
	print 'Checking message...'

	# Check for expected input files before beginning processing
	if has_all_files(parameters):
		if has_been_handled(parameters):
			print 'skipping, dataset already handled'
			return False
		else:
			# Handle the message but do not download any files automatically.
			return True
	else:
		print 'skipping, not all input files are ready'
		return False

# Get stream ID from Clowder based on stream name
def get_stream_id(host, key, name):
	url = "%sapi/geostreams/streams?stream_name:'%s'&key=%s" % (host, name, key)

	print("...searching for stream ID: "+url)
	r = requests.get(url)
	if (r.status_code != 200):
		print("ERR  : Problem searching stream ID : [" + str(r.status_code) + "] - " + r.text)
	else:
		json_data = r.json()
		for s in json_data:
			if 'name' in s and s['name'] == name:
				return s['id']
			else:
				print("error searching for stream ID")

	return None

#! Save records as JSON back to GeoStream.
# @see {@link https://opensource.ncsa.illinois.edu/bitbucket/projects/GEOD/repos/seagrant-parsers-py/browse/SeaBird/seabird-import.py}
def upload_records(host, key, records):
	url = "%sapi/geostreams/datapoints?key=%s" % (host, key)

	for record in records:
		headers = {'Content-type': 'application/json'}
		r = requests.post(url, data=json.dumps(record), headers=headers)
		if (r.status_code != 200):
			print 'ERR : Problem creating datapoint : [%s] - %s' % (str(r.status_code), r.text)
	return

# ----------------------------------------------------------------------
# Process the dataset message and upload the results
def process_dataset(parameters):
	global parse_file, extractorName, inputDirectory, outputDirectory, sensorId, streamName, ISO_8601_UTC_OFFSET

	host = parameters['host']
	if not host.endswith('/'):
		host += "/"

	# Look for stream.
	streamId = get_stream_id(host, parameters['secretKey'], streamName)
	if streamId == None:
		raise LookupError('Unable to find stream with name "%s".' % streamName)
#! The following is not Working.
# 		headers = {'Content-type': 'application/json'}
# 		body = {
# 			"name": streamName,
# 			"type": "point",
# 			"geometry": {
# 				"type": "Point",
# 				"coordinates": [0, 0, 0]
# 			},
# 			"properties": {},
# 			"sensor_id": str(sensorId)
# 		}
# 		r = requests.post("%s/api/geostreams/streams?key=%s" % (parameters['host'], parameters['secretKey']), data=json.dumps(body), headers=headers)
# 		if (r.status_code != 200):
# 			print("ERR  : Problem creating stream : [" + str(r.status_code) + "] - " + r.text)

	# Find input files in dataset
	fileExt = '.dat'
	files = get_all_files(parameters)[fileExt]

	datasetUrl = '%sdatasets/%s' % (host, parameters['datasetId'])

	# Process each file and concatenate results together.
	for file in files:
		# Find path in parameters
		for f in parameters['files']:
			if os.path.basename(f) == file['filename']:
				filepath = f

		# Parse one file and get all the records in it.
		records = parse_file(filepath, utc_offset=ISO_8601_UTC_OFFSET)

		# Add props to each record.
		for record in records:
			record['source'] = datasetUrl
			record['file'] = file['id']
			record['sensor_id'] = str(sensorId)
			record['stream_id'] = str(streamId)

		upload_records(host, parameters['secretKey'], records)

	# Mark dataset as processed.
	metadata = {
		"@context": {
			"@vocab": "https://clowder.ncsa.illinois.edu/clowder/assets/docs/api/index.html#!/files/uploadToDataset"
		},
		"dataset_id": parameters["datasetId"],
		"content": {"status": "COMPLETED"},
		"agent": {
			"@type": "cat:extractor",
			"name": extractorName
		}
	}
	extractors.upload_dataset_metadata_jsonld(mdata=metadata, parameters=parameters)

	print("processing completed")
# ----------------------------------------------------------------------
# Find as many expected files as possible and return the set.
def get_all_files(parameters):
	global requiredInputFiles

	# `files` is a dictionary of arrays of file descriptors.
	# files: {Dict.<List.<File>>}
	files = dict()
	for fileExt in requiredInputFiles:
		files[fileExt] = []

	if 'filelist' in parameters:
		for fileItem in parameters['filelist']:
			fileId   = fileItem['id']
			fileName = fileItem['filename']
			for fileExt in files:
				if fileName[-len(fileExt):] == fileExt:
					files[fileExt].append({
						'id': fileId,
						'filename': fileName
					})
	return files

# ----------------------------------------------------------------------
# Returns the output filename.
def get_output_filename(raw_filename):
	return '%s.nc' % raw_filename[:-len('_raw')]

# ----------------------------------------------------------------------
# Returns true if all expected files are found.
def has_all_files(parameters):
	global requiredInputFiles

	files = get_all_files(parameters)
	allFilesFound = True
	for fileExt in requiredInputFiles:
		if len(files[fileExt]) < requiredInputFiles[fileExt]:
			allFilesFound = False
	return allFilesFound

# ----------------------------------------------------------------------
# Returns true if the dataset has been handled.
def has_been_handled(parameters):
	global extractorName

	if 'filelist' not in parameters:
		return False
	if not has_all_files(parameters):
		return False
	# Check metadata.
	md = extractors.download_dataset_metadata_jsonld(parameters['host'], parameters['secretKey'], parameters['datasetId'], extractorName)
	for m in md:
		if 'agent' in m and 'name' in m['agent'] and m['agent']['name'] == extractorName:
			return True

	return False

if __name__ == "__main__":
	main()
