#!/usr/bin/python

import math
import datetime
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

def parse_file_header_line(linestr):
	return map(lambda x: json.loads(x), str(linestr).split(','))

# ----------------------------------------------------------------------
# Parse the CSV file and return a list of dictionaries.
def parse_file(filepath, utc_offset = 'Z'):
	results = []
	with open(filepath) as csvfile:
		# First line is always the header.
		# @see {@link https://www.manualslib.com/manual/538296/Campbell-Cr9000.html?page=41#manual}
		header_lines = [
			csvfile.readline()
		]

		file_format, station_name, logger_model, logger_serial, os_version, dld_file, dld_sig, table_name = parse_file_header_line(header_lines[0])

		if file_format != 'TOA5':
			raise ValueError('Unsupported format "%s".' % file_format)

		# For TOA5, there are in total 4 header lines.
		# @see {@link https://www.manualslib.com/manual/538296/Campbell-Cr9000.html?page=43#manual}
		while (len(header_lines) < 4):
			header_lines.append(csvfile.readline())

		prop_names = parse_file_header_line(header_lines[1])
		prop_units = parse_file_header_line(header_lines[2])
		prop_sample_method = parse_file_header_line(header_lines[3])

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
			timestamp = datetime.datetime.strptime(row['TIMESTAMP'], '%Y-%m-%d %H:%M:%S').isoformat() + str(utc_offset)
			newResult = {
				'start_time': timestamp,
				'end_time': timestamp,
				'properties': transformProps(props, row),
				'type': 'Feature',
				'geometry': {
					'type': 'Point',
					'coordinates': [
						# SW Corner.
						# @see {@link https://github.com/terraref/extractors-metadata/blob/master/sensorposition/terra.sensorposition.py#L68}
						33.0745666667,
						-111.9750833333,
						0
					]
				}
			}
			newResult['properties']['_raw'] = {
				'data': row,
				'units': prop_units,
				'sample_method': prop_sample_method
			}
			results.append(newResult)
	return results

if __name__ == "__main__":
	sample_file = '../testdata/WeatherStation_SecData_2016_08_29_2304.dat'
	print json.dumps(parse_file(sample_file))