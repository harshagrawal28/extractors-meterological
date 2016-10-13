#!/usr/bin/python

import math
import csv
import json

def tempUnit2K(value, unit):
	if unit == 'Deg C':
		return value + 273.15
	elif unit == 'Deg F':
		return (value + 459.67) * 5 / 9
	elif unit == 'Deg K':
		return value
	else:
		raise ValueError('Unsupported unit "%s".' % unit)

def relHumidUnit2Percent(value, unit):
	if unit == '%':
		return value
	else:
		raise ValueError('Unsupported unit "%s".' % unit)

def speedUnit2MeterPerSecond(value, unit):
	if unit == 'meters/second':
		return value
	else:
		raise ValueError('Unsupported unit "%s".' % unit)

def extractXFactor(magnitude, degreeFromNorth):
	return magnitude * math.sin(math.radians(degreeFromNorth));
def extractYFactor(magnitude, degreeFromNorth):
	return magnitude * math.cos(math.radians(degreeFromNorth));

# 'AirTC': 'air_temperature',
# 'RH': 'relative_humidity',
# 'Pyro': 'surface_downwelling_shortwave_flux_in_air',
# 'PAR_ref': 'surface_downwelling_photosynthetic_photon_flux_in_air',
# 'WindDir': 'wind_to_direction',
# 'WS_ms': 'wind_speed',
# 'Rain_mm_Tot': 'precipitation_rate'

# Each mapping function can decide to return one or multiple tuple, so leave the list to them.
PROP_MAPPING = {
	'AirTC': lambda d: [(
		'air_temperature',
		tempUnit2K(float(d['value']), d['meta']['unit'])
	)],
	'RH': lambda d: [(
		'relative_humidity',
		relHumidUnit2Percent(float(d['value']), d['meta']['unit'])
	)],
	'Pyro': lambda d: [(
		'surface_downwelling_shortwave_flux_in_air',
		float(d['value'])
	)],
	'PAR_ref': lambda d: [(
		'surface_downwelling_photosynthetic_photon_flux_in_air',
		float(d['value'])
	)],
	# If Wind Direction is present, split into speed east and speed north it if we can find Wind Speed.
	'WindDir': lambda d: [
		('eastward_wind', extractXFactor(float(d['record']['WS_ms']), float(d['value']))),
		('northward_wind', extractYFactor(float(d['record']['WS_ms']), float(d['value'])))
	],
	# If Wind Speed is present, process it if we can find Wind Direction.
	'WS_ms': lambda d: [(
		'wind_speed',
		speedUnit2MeterPerSecond(float(d['value']), d['meta']['unit'])
	)],
	'Rain_mm_Tot': lambda d: [(
		'precipitation_rate',
		float(d['value'])
	)]
}

def transformProps(propMetaDict, propValDict):
	newProps = []
	for propName in propValDict:
		if propName in PROP_MAPPING:
			newProps += PROP_MAPPING[propName]({
				'meta': propMetaDict[propName],
				'value': propValDict[propName],
				'record': propValDict
			})
	return dict(newProps)

# ----------------------------------------------------------------------
# Parse the CSV file and return a list of dictionaries.
def parse_file(filepath):
	results = []
	with open(filepath) as csvfile:
		# Consume first 4 lines.
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
				'title': prop_names[x],
				'unit': prop_units[x],
				'sample_method': prop_sample_method[x]
			}
		# [DEBUG] Print the property details if needed.
		#print json.dumps(props)

		reader = csv.DictReader(csvfile, fieldnames=prop_names)
		for row in reader:
			results.append({
				'id': '???',
				'created': '???',
				'start_time': '???', #! How to format time?
				'end_time': '???',
				'properties': transformProps(props, row),
# 				dict( map(lambda key: ( PROP_MAPPING[key], {
# 					'value': row[key],
# 					'unit': props[key]['unit'],
# 					'sample_method': props[key]['sample_method']
# 				} ), list(set(prop_names) & set(PROP_MAPPING))) ),
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
