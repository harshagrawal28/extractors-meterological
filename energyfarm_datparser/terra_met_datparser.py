
import os
import csv
import json
import requests
import urllib
import urlparse
import logging

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files
import pyclowder.datasets

from parser import *


class MetDATFileParser(Extractor):
	def __init__(self):
		Extractor.__init__(self)

		# parse command line and load default logging configuration
		self.setup()

		# setup logging for the exctractor
		logging.getLogger('pyclowder').setLevel(logging.DEBUG)
		logging.getLogger('__main__').setLevel(logging.DEBUG)


	def check_message(self, connector, host, secret_key, resource, parameters):
		# Not completed yet #
		
		return CheckMessage.download

	def process_message(self, connector, host, secret_key, resource, parameters):
		ISO_8601_UTC_OFFSET = dateutil.tz.tzoffset("-07:00", -7 * 60 * 60)
	
		# Get input files
		logger = logging.getLogger(__name__)
		inputfile = resource["local_paths"][0]
		fileId = resource['id']	
			
		filename = resource['name']

		sensor_name = 'Energy Farm Met Station '
		stream_name = 'weather station '

		if 'CEN' in filename:
			sensor_name+= 'CEN'
			stream_name+= 'CEN'
			main_coords = [40.062051,-88.199801,0]
		elif 'NE' in filename:
			sensor_name+= 'NE'
			stream_name+= 'NE'
			main_coords = [40.067379,-88.193298,0]
		elif 'SE' in filename:
			sensor_name+= 'SE'
			stream_name+= 'SE'
			main_coords = [40.056910,-88.193573,0]

		# SENSOR is Full Field by default
		sensor_id = get_sensor_id(host, secret_key, sensor_name)
		if not sensor_id:
			sensor_id = create_sensor(host, secret_key, sensor_name, {
				"type": "Point",
				# These are a point off to the right of the field
				"coordinates": main_coords
			})		
		

		# Look for stream.
		
		stream_id = get_stream_id(host, secret_key, stream_name)
		if not stream_id:
			stream_id = create_stream(host, secret_key, sensor_id, stream_name, {
				"type": "Point",
				"coordinates": [0,0,0]
			})
		
		# Get metadata to check till what time the file was processed last. Start processing the file after this time
		md = pyclowder.files.download_metadata(connector, host, secret_key, resource['id'], self.extractor_info['name'])
		if md != [] and 'content' in md[0] and 'last processed time' in md[0]['content']:
			last_processed_time = md[0]['content']['last processed time']
			delete_metadata(connector, host, secret_key, resource['id'], self.extractor_info['name'])
		else:
			last_processed_time = 0				


		# Parse file and get all the records in it.
		records = parse_file(inputfile, last_processed_time,utc_offset=ISO_8601_UTC_OFFSET)
		# Add props to each record.
		for record in records:
			record['properties']['source_file'] = fileId
			record['stream_id'] = str(stream_id)
		
		upload_datapoints(host, secret_key, records)

		last_processed_time = records[-1]["end_time"]

		metadata = {
			"@context": ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"],
			"dataset_id": resource['id'],
			"content": {"status": "COMPLETED",
				    "last processed time": last_processed_time					
				},
			"agent": {
				"@type": "extractor",
				"extractor_id": host + "/api/extractors/" + self.extractor_info['name']
			}
		}

		# logger.debug(metadata)
		pyclowder.files.upload_metadata(connector, host, secret_key, resource['id'], metadata)


# Get sensor ID from Clowder based on plot name
def get_sensor_id(host, key, name):
	if(not host.endswith("/")):
		host = host+"/"

	url = "%sapi/geostreams/sensors?sensor_name=%s&key=%s" % (host, name, key)
	logging.debug("...searching for sensor : "+name)
	r = requests.get(url)
	if r.status_code == 200:
		json_data = r.json()
		for s in json_data:
			if 'name' in s and s['name'] == name:
				return s['id']
	else:
		print("error searching for sensor ID")

	return None

def create_sensor(host, key, name, geom):
	if(not host.endswith("/")):
		host = host+"/"

	body = {
		"name": name,
		"type": "point",
		"geometry": geom,
		"properties": {
			"popupContent": name,
			"type": {
				"id": "Met Station",
				"title": "Met Station",
				"sensorType": 4
			},
			"name": name,
			"region": "Urbana"
		}
	}

	url = "%sapi/geostreams/sensors?key=%s" % (host, key)
	logging.info("...creating new sensor: "+name)
	r = requests.post(url,
					  data=json.dumps(body),
					  headers={'Content-type': 'application/json'})
	if r.status_code == 200:
		return r.json()['id']
	else:
		logging.error("error creating sensor")

	return None


# Save records as JSON back to GeoStream.
def upload_datapoints(host, key, records):
	url = urlparse.urljoin(host, 'api/geostreams/datapoints?key=%s' % key)
	
	for record in records:
		headers = {'Content-type': 'application/json'}
		r = requests.post(url, data=json.dumps(record), headers=headers)
		if (r.status_code != 200):
			logging.error('Problem creating datapoint : [%s] - %s' % (str(r.status_code), r.text))
	return


def delete_metadata(connector, host, key, fileid, extractor=None):
    """Delete file JSON-LD metadata from Clowder.
    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file to fetch metadata of
    extractor -- extractor name to filter results (if only one extractor's metadata is desired)
    """
    filterstring = "" if extractor is None else "&extractor=%s" % extractor
    url = '%sapi/files/%s/metadata.jsonld?key=%s%s' % (host, fileid, key, filterstring)
    # fetch data
    result = requests.delete(url, stream=True,
                          verify=connector.ssl_verify)
    result.raise_for_status()
    return result.json()

# Get stream ID from Clowder based on stream name
def get_stream_id(host, key, name):
	if(not host.endswith("/")):
		host = host+"/"

	url = urlparse.urljoin(host, 'api/geostreams/streams?stream_name=%s&key=%s' % (name, key))
	logging.debug("...searching for stream : "+name)
	r = requests.get(url)
	if r.status_code == 200:
		json_data = r.json()
		for s in json_data:
			if 'name' in s and s['name'] == name:
				return s['id']
	else:
		logging.error("error searching for stream ID")

	return None

def create_stream(host, key, sensor_id, name, geom):
	if(not host.endswith("/")):
		host = host+"/"
	body = {
		"name": name,
		"type": "Feature",
		"geometry": geom,
		"properties": {},
		"sensor_id": str(sensor_id)
	}
	
	url = urlparse.urljoin(host, 'api/geostreams/streams?key=%s' % key)

	logging.info("...creating new stream: "+name)
	r = requests.post(url,
					  data=json.dumps(body),
					  headers={'Content-type': 'application/json'})
	if r.status_code == 200:
		return r.json()['id']
	else:
		logging.error("error creating stream")

	return None

if __name__ == "__main__":
	extractor = MetDATFileParser()
	extractor.start()
