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
import subprocess
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

# ----------------------------------------------------------------------
# Process the dataset message and upload the results
def process_dataset(parameters):
	global parse_file, extractorName, inputDirectory, outputDirectory, sensorId, streamId

	print 'Extractor Running'

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
		records += parse_file(file['path'], sensorId, streamId);

	print records

	#! Save records as JSON back to GeoStream.
	# @see {@link https://opensource.ncsa.illinois.edu/bitbucket/projects/GEOD/repos/seagrant-parsers-py/browse/SeaBird/seabird-import.py}

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
	if 'filelist' not in parameters:
		return False
	if not has_all_files(parameters):
		return False
	#! Check metadata.
	return False
# 	files = get_all_files(parameters)
# 	outFilename = get_output_filename(files['_raw']['filename'])
# 	outFileFound = False
# 	for fileItem in parameters['filelist']:
# 		if outFilename == fileItem['filename']:
# 			outFileFound = True
# 			break
# 	return outFileFound

if __name__ == "__main__":
	main()
