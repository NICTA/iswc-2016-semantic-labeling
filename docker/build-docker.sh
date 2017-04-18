#!/bin/bash
#
# This script will build the uber-jar on the host machine, then
# run a docker script that contains only the jar and the executable.
# Additionally, the docker image is exported to a .tar.gz file for
# use on other machines.
#

echo "Building docker image..."

docker build -t karma .
if [ $? -eq 0 ]; then
    echo "Docker image constructed successfully."
else
    echo "Docker failed to build"
    exit 1
fi

#
# output docker to image...
#
FILENAME=karma-$(git rev-parse HEAD | cut -c1-8).tar

echo "Exporting docker to $FILENAME..."
docker save --output $FILENAME karma
if [ $? -eq 0 ]; then
    echo "Compressing file..."
    gzip $FILENAME
    echo "Docker image exported successfully to $FILENAME.gz"
else
    echo "Docker failed to export to $FILENAME"
    exit 1
fi

#
# Notify user...
#
echo ""
echo "File exported to $FILENAME.gz. Copy onto remote machine and restore with:"
echo ""
echo " docker load --input $FILENAME.gz"
echo ""
echo "Launch with:"
echo ""
echo " docker run -d -p 8000:8000 --name karma-instance karma &"
echo ""
echo "Test with:"
echo ""
echo " curl :9000/"
echo ""
echo "Check status with:"
echo ""
echo " docker ps"
echo ""
echo "Stop server with:"
echo ""
echo " docker stop karma-instance -t 0"
echo ""
echo "Restart with:"
echo ""
echo " docker start karma-instance"
echo ""
