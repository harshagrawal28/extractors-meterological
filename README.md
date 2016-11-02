# Meteorological extractors

This repository contains extractors that process and derive outputs from weather station instruments. 


### DAT parser extractor
This extractor extracts metadata from meteorological DAT files into netCDF, as well as creating entries in the Clowder Geostreams database.

_Input_

  - Evaluation is triggered whenever 24 .dat files are added to a dataset
  			
_Output_

  - netCDF metadata is generated and added to dataset
  - datapoints for each record in the DAT files are added to geostream
  