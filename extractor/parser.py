#!/usr/bin/python

import csv
import json

# ----------------------------------------------------------------------
# Parse the CSV file and return a list of dictionaries.
def parse_file(filepath):
	results = []
	with open(filepath) as csvfile:
		# Skip first 4 lines.
		line1 = csvfile.readline()
		line2 = csvfile.readline()
		line3 = csvfile.readline()
		line4 = csvfile.readline()
		
		prop_names = map(lambda x: x.split('"')[1], line2.split(','))
		prop_units = map(lambda x: x.split('"')[1], line3.split(','))
		prop_sample_method = map(lambda x: x.split('"')[1], line4.split(','))
		
		# Associate the above lists.
		props = dict()
		for x in xrange(len(prop_names)):
			props[prop_names[x]] = {
				'unit': prop_units[x],
				'sample_method': prop_sample_method[x]
			}

		#! fieldnames is from line2.
		reader = csv.DictReader(csvfile, fieldnames=prop_names)
		for row in reader:
			results.append({
				'id': '???',
				'created': '???',
				'start_time': '???', #! How to format time?
				'end_time': '???',
				'properties': dict( map(lambda key: ( key, {
					'value': row[key],
					'unit': props[key]['unit'],
					'sample_method': props[key]['sample_method']
				} ), prop_names) ),
				'type': 'Feature',
				'geometry': {
					'type': 'Point',
					'coordinates': [
						'???',
						'???',
						'???'
					]
				},
				'stream_id': '???',
				'sensor_id': '???', #! Site?
				'sensor_name': '???' #! Site?
			})
	return results

if __name__ == "__main__":
	sample_file = '../testdata/WeatherStation_SecData_2016_08_29_2304.dat'
	print json.dumps(parse_file(sample_file))
