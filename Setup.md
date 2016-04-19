# Setting up PROVR

Deploying the PROVR monitoring system is straight forward and easy, you can run
backend and frontend on a single server or separately on distinct nodes (+ a node
for the database, if you want). We recommend using _virtualenv_ with Python and
to use _pip_ to install required libraries within this environment to keep your
local systems Python installation untouched.

## Preliminaries

This software is under development and testing on Linux Debian 8 (Jessie).

On Debian the following packages can be installed via apt-get or aptitude:

 - libxml2-dev,         needed by python for xml parsing and bgpmon
 - python-dev,          needed to build and install python libraries via pip
 - python-pip,          a package manager for python
 - python-virtualenv,   run python code in a change-root like environment

additional, but optional:
 - screen,              terminal/shell multiplexer
 - vim,                 the editor

Install shutcut:

    # apt-get install libxml2-dev python-dev python-pip python-virtualenv
    # apt-get install screen vim

On other Linux Distros search for equivalents in their package-management.

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

If you want to install and setup your own BGPmon instance see below for further
details.

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

## Apache and Systemd integration

For a production deployment we recommend to integrate PROVR with _Apache_,
instead of the python standalone webserver described in the previous section.
The PROVR backend will run as a _Systemd_ service daemon, we provide the
necessary scripts as well.

In the following we describe Apache integration assuming the steps above are
already followed through. First install Apache and its WSGI module (`mod_wsgi`)
via package manager of your Linux OS, e.g., `apt` or `yum`. Afterwards proceed
as follows:

1. Edit `src/provr.wsgi` and replace the `</path/to/provr>` according to your deployment.
2. Copy `etc/httpd/conf.d/provr_wsgi.conf` to the Apache config directory, e.g.,
`/etc/httpd/conf.d/`. And again replace `</path/to/provr>` according to your
deployment.
3. Modify `src/settings.py` as required as well, if not already done.
4. Set the absolute path to `README.md` in `src/app/views.py` (L 44)
5. Modify `src/app/config.py` if required, e.g., match database connection.
6. Copy _Systemd_ config for PROVR backend `etc/systemd/system/provr.service` to
`/etc/systemd/system`
7. Modify `/etc/systemd/system/provr.service` and replace `</path/to/provr>`.
8. Enable PROVR service:
```
# systemctl daemon-reload
# systemctl enable provr.service
# systemctl start provr.service
```
9. Reload _Apache_:
```
# systemctl reload httpd
```


## BGPmon

At the moment `bgpmon` cannot be found in standard package repos. So you
need to compile and install it from scratch. Its source code can be downloaded
[here](http://www.bgpmon.io/download.html).

Compile with `./configure && make`, optional `sudo make install`.

_Note_: there is bug in bgpmon-7.4 causing segfaults when connecting to multiple
bgp peers, but luckily we provide a patch for that. Apply the patch as follows:

    $ cd /path/to/bgpmon-7.4-source
    $ patch -p1 < /path/to/provr/src/bgpmon/createSessionStruct.patch
    $ ./configure
    $ make
    $ sudo make install

A configuration example for BGPmon is provided in [bgpmon_config.txt](src/bgpmon/bgpmon_config.txt).
