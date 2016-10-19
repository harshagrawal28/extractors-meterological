# =============================================================================
#
# In order for this extractor to run according to your preferences,
# the following parameters need to be set.
#
# Some parameters can be left with the default values provided here - in that
# case it is important to verify that the default value is appropriate to
# your system. It is especially important to verify that paths to files and
# software applications are valid in your system.
#
# =============================================================================

import os

# name to show in rabbitmq queue list
extractorName = os.getenv('RABBITMQ_QUEUE', "terra.meterological")

# URL to be used for connecting to rabbitmq
rabbitmqURL = os.getenv('RABBITMQ_URI', "amqp://guest:guest@localhost/%2f")

# name of rabbitmq exchange
rabbitmqExchange = os.getenv('RABBITMQ_EXCHANGE', "clowder")

# type of files to process
messageType = "*.dataset.file.added"

# trust certificates, set this to false for self signed certificates
sslVerify = os.getenv('RABBITMQ_SSLVERIFY', False)

# Workspace for input/output files.
inputDirectory = os.getenv('INPUTDIR', "./input")
outputDirectory = os.getenv('OUTPUTDIR', "./output")

# The extractor will only run when all these files are present.
# These are just filename postfixes for file matching.
# We need 24 `.dat` files.
requiredInputFiles = {
	'.dat': 24
}

restEndPoint = os.getenv('CLOWDER_URI', "http://localhost:9001")

sensorId = -1 #! Put real value here. Somebody has to ensure this exists.
streamName = "weather station"
