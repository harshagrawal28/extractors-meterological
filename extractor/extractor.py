#!/usr/bin/env python

"""
terra.meterological.py

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
	global extractorName, messageType, rabbitmqExchange, rabbitmqURL

	# Set logging
	logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
	logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)

	# Connect to rabbitmq
	extractors.connect_message_bus(
		extractorName        = extractorName,
		messageType          = messageType,
		rabbitmqExchange     = rabbitmqExchange,
		rabbitmqURL          = rabbitmqURL,
		processFileFunction  = process_dataset,
		checkMessageFunction = check_message
	)

def check_message(parameters):
	# Check for expected input files before beginning processing
	if has_all_files(parameters):
		if has_been_handled(parameters):
			print 'skipping, dataset already handled'
			return False
		else:
			# Handle the message but do not download any files automatically.
			return "bypass"
	else:
		print 'skipping, not all input files are ready'
		return False

# Get stream ID from Clowder based on stream name
def get_stream_id(host, key, name):

	url = urlparse.urljoin(host, 'api/geostreams/streams?%s' % urllib.urlencode({
		"stream_name": name,
		"key": key
	}))

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

# Check if the dataset has the tag.
def dataset_has_tag(host, datasetId, tag, key):

	url = urlparse.urljoin(host, 'api/datasets/%s/tags?key=%s' % (datasetId, key))

	print 'Check Dataset Tag: ' + url

	headers = {'Content-type': 'application/json'}
	r = requests.get(url, headers=headers)
	if (r.status_code != 200):
		print("ERR  : Problem getting dataset tags : [" + str(r.status_code) + "] - " + r.text)
	else:
		json_data = r.json()
		if tag in json_data['tags']:
			return True
	return False

# Add a tag to the dataset.
def dataset_add_tag(host, datasetId, tag, key):
	global extractorName

	url = urlparse.urljoin(host, 'api/datasets/%s/tags?key=%s' % (datasetId, key))

	headers = {'Content-type': 'application/json'}
	body = {
		"extractor_id": extractorName,
		"tags": [
			tag
		]
	}
	r = requests.post(url, data=json.dumps(body), headers=headers)
	if (r.status_code != 200):
		print("ERR  : Problem adding dataset tag : [" + str(r.status_code) + "] - " + r.text)
	else:
		return True
	return False

# ----------------------------------------------------------------------
# Process the dataset message and upload the results
def process_dataset(parameters):
	global parse_file, extractorName, inputDirectory, outputDirectory, filter_tag, sensorId, streamName, ISO_8601_UTC_OFFSET

	print 'Extractor Running'

	# Look for stream.
	streamId = get_stream_id(parameters['host'], parameters['secretKey'], streamName)
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

	# Download files to input directory
	for file in files:
		file['path'] = extractors.download_file(
			channel            = parameters['channel'],
			header             = parameters['header'],
			host               = parameters['host'],
			key                = parameters['secretKey'],
			fileid             = file['id'],
			# What's this argument for?
			intermediatefileid = file['id'],
			ext                = fileExt
		)
		# Restore temp filenames to original - script requires specific name formatting so tmp names aren't suitable
		file['old_path'] = file['path']
		file['path'] = os.path.join(inputDirectory, file['filename'])
		os.rename(file['old_path'], file['path'])
		print 'found %s file: %s' % (fileExt, file['path'])

	#print json.dumps(files)

	# Process each file and concatenate results together.
	records = []
	for file in files:
		newRecord = parse_file(file['path'], utc_offset=ISO_8601_UTC_OFFSET)
		newRecord['sensor_id'] = str(sensorId)
		newRecord['stream_id'] = str(streamId)
		records += newRecord

	print json.dumps(records)

	#! Save records as JSON back to GeoStream.
	# @see {@link https://opensource.ncsa.illinois.edu/bitbucket/projects/GEOD/repos/seagrant-parsers-py/browse/SeaBird/seabird-import.py}
	for record in records:
		headers = {'Content-type': 'application/json'}
		r = requests.post("%s/api/geostreams/datapoints?key=%s" % (restEndPoint, parameters['secretKey']), data=json.dumps(record), headers=headers)
		if (r.status_code != 200):
			print("ERR  : Problem creating datapoint : [" + str(r.status_code) + "] - " + r.text)

	# Mark dataset as processed.
	dataset_add_tag(parameters['host'], parameters['datasetId'], filter_tag, parameters['secretKey'])

	print 'cleaning up...'
	# Clean up the input files.
	for file in files:
		os.remove(file['path'])
	print 'done cleaning'

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
	global filter_tag

	if 'filelist' not in parameters:
		return False
	if not has_all_files(parameters):
		return False
	# Check tags.
	if dataset_has_tag(parameters['host'], parameters['datasetId'], filter_tag, parameters['secretKey']):
		return True
	return False

if __name__ == "__main__":
	main()
