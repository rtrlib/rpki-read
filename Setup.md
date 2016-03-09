# Setting up PROVR

Deploying the PROVR monitoring system is straight forward and easy, you can run
backend and frontend on a single server or separately on distinct nodes (+ a node
for the database, if you want). We recommend using _virtualenv_ with Python and
to use _pip_ to install required libraries within this environment to keep your
local systems Python installation untouched.

## Backend

The PROVR backend consists of 3 components:

1. the [parser](src/bgpmonUpdateParser.py), to extract prefix origins from a XML BGP update stream
2. the [validator](src/validator.py), to validate prefix origins against an RPKI cache
3. the [database](src/dbHandler.py), to store latest validation results

Each of these components is implemented as a standalone python tool, i.e., they
run on their own and are interchangeable. They follow common UNIX tools, i.e,
read input from STDIN and write to STDOUT if feasible. Thus, the complete PROVR
backend is basically running these 3 tools in a chain.

The validation results are stored in a database, PROVR currently uses a MongoDB.
This database is also used by the web frontend to display validation results and
statistics.

### requirements

The backend mostly uses standard Python libraries however to parse IP prefixes
and addresses we use _netaddr_, and for the mongodb database PROVR requires
_pymongo_.

Besides that you need access to a [BGPmon](http://www.bgpmon.io) instance to
receive its BGP update stream.
And you will also need the URL of a RPKI cache for the validation procedure.

### run

For the backend process run the following command chain, replace respective
addresses and ports as needed:

```
python bgpmonUpdateParser.py -a <bgpmon-addr> -p <bgpmon-port> | \
python validator.py -a <rpki-cache-addr> -p <rpki-cache-port> | \
python dbHandler.py -m <mongodb-URI>
```

A mongodb-URI looks something like `mongodb://<host>:<port>/<dbname>`.
To configure the backend have a look at the [settings](src/settings.py).

The 'bgpmonUpdateParser' also supports to read the _RIB_ XML stream of a bgpmon
instance first, before it starts to parse the BGP update stream. This way you
fill the database with all currently known IP prefixes and their origin AS,
including validation. To activate and use this feature specify the RIB XML
stream port using the additional '-r <rib port>' parameter.

## Frontend

The PROVR monitoring frontend provides a web GUI to view validation stats and
results of currently announced IP prefixes and the respective origin AS.

### requirements

The frontend uses _Flask_ for all the web stuff, _netaddr_ to parse IP prefixes
and addresses, and _pymongo_ for the database connection. Besides that you need
access to the database of the PROVR backend, i.e., its URI and authentication
params (if required).

### run

To initialize the frontend check its configuration params in [config.py](src/app/config.py),
afterwards exec:

```
python webfrontend.py
```

The webfrontend runs on port 'localhost:5000' you may alter the port or setup
a webproxy (e.g. 'nginx') to redirect traffic.
