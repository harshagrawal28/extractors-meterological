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
import subprocess
import logging
from config import *
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
	global extractorName, workerScript, inputDirectory, outputDirectory

	print parameters
	print 'Extractor Running'
	return False

	# Find input files in dataset
	files = get_all_files(parameters)

	# Download files to input directory
	for fileExt in files:
		files[fileExt]['path'] = extractors.download_file(
			channel            = parameters['channel'],
			header             = parameters['header'],
			host               = parameters['host'],
			key                = parameters['secretKey'],
			fileid             = files[fileExt]['id'],
			# What's this argument for?
			intermediatefileid = files[fileExt]['id'],
			ext                = fileExt
		)
		# Restore temp filenames to original - script requires specific name formatting so tmp names aren't suitable
		files[fileExt]['old_path'] = files[fileExt]['path']
		files[fileExt]['path'] = os.path.join(inputDirectory, files[fileExt]['filename'])
		os.rename(files[fileExt]['old_path'], files[fileExt]['path'])
		print 'found %s file: %s' % (fileExt, files[fileExt]['path'])

	# Invoke terraref.sh
	outFilePath = os.path.join(outputDirectory, get_output_filename(files['_raw']['filename']))
	print 'invoking terraref.sh to create: %s' % outFilePath
	returncode = subprocess.call(["bash", workerScript, "-d", "1", "-I", inputDirectory, "-O", outputDirectory])
	print 'done creating output file (%s)' % (returncode)

	if returncode != 0:
		print 'terraref.sh encountered an error'

	# Verify outfile exists and upload to clowder
	if os.path.exists(outFilePath):
		print 'output file detected'
		if returncode == 0:
			print 'uploading output file...'
			extractors.upload_file_to_dataset(filepath=outFilePath, parameters=parameters)
			print 'done uploading'
		# Clean up the output file.
		os.remove(outFilePath)
	else:
		print 'no output file was produced'

	print 'cleaning up...'
	# Clean up the input files.
	for fileExt in files:
		os.remove(files[fileExt]['path'])
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
