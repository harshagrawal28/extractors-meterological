# extractors-meterological
**This repo contains submodules so make sure to pull all of them to the latest version.**

## Local development
1. To bring up the local Docker containers for the rabbitMQ, mongoDB and Clowder servers:

        $> . clowder/start

2. To stop those containers:

        $> . clowder/stop

3. To get registration link for Clowder:

        $> . clowder/show-signups

4. To build the extractor Docker image:

        $> . extractor/build

5. To run the extractor Docker container: (This uses the rabbitMQ server started in step 1. If you don't need that server, this shortcut prints out the command it runs so you can modify that yourself.)

        $> . extractor/run

### Connecting to a remote server
Modify the rabbitMQ related environment variables at the end of `extractor/Dockerfile`.

### Lite testing
This extractor looks for specific amount of specific files in the datasets to decide whether to ignore the message or not.

For testing, you might not want to wait for too many files to be ready. Modify the `requiredInputFiles` dictionary variable in `extractor/config.py` to suit your testing needs. Just remember to not commit those changes.

## Merging into Master
You might want to move the content of folder `extractor` into the root folder. The other files are for local testing only and should be discarded.
