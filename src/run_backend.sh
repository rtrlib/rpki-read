#!/bin/bash

PYTHON_EXEC=$(which python2.7)
READLINK=$(which greadlink)
[ -z "$READLINK" ] && {
	READLINK=$(which readlink)
}
SCRIPT_PATH=$(dirname "$($READLINK -f "$0")")
echo "$SCRIPT_PATH"
cd $SCRIPT_PATH
# better wait for other service to get ready first
sleep 42
$PYTHON_EXEC bgpmonUpdateParser.py | $PYTHON_EXEC validator.py | $PYTHON_EXEC dbHandler.py -p
