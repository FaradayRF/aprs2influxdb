# aprs2influxdb
This program interfaces ham radio APRS-IS servers to connect into the stream of station packets transferred over the network. aprs2influxdb handles the connection, parsing, and saving of data into an influxdb database using line protocol formatted strings. Periodically, a status message is also sent to the APRS-IS server in order to maintain the connection with the APRS-IS server by preventing a timeout.

## Getting started
aprs2influxdb installs using pip can can be installed in editable mode with the source code or from [PyPI](https://pypi.python.org/pypi).

### Prerequisites
You must install and configure an [influxdb](https://www.influxdata.com/) database. Here is their open source [project documentation on GitHub](https://github.com/influxdata/influxdb).

### Installing
#### PyPI
`pip install aprs2influxdb`

#### Source Code
If you are just installing with source code then navigate to the source directory and run:

`pip install .`

if you are installing in editable mode for development then navigate to the source directory and run:

`pip install -e .`

### Running aprs2influxdb
The program defaults use standard influxdb login information as well as example APRS-IS login information. If you properly installed influxdb you will need to specify your own database information. Additionally, you will need an amateur radio license with which you may login to APRS-IS with your callsign.

#### Command Line Options

* `--help` show this help message and exit
* `--dbhost DBHOST` set influxdb host (default = localhost)
* `--dbport DBPORT` set influxdb port (default = 8086)
* `--dbuser DBUSER` set influxdb user (default = root)
* `--dbpassword DBPASSWORD` set influxdb password (default = root)
* `--dbname DBNAME` set influxdb database name (default = mydb)
* `--callsign CALLSIGN` set APRS-IS login callsign (default = nocall)
* `--port PORT` set APRS-IS port (default = 10152)
* `--interval INTERVAL` set APRS-IS heartbeat interval in minutes (default = 15)

#### Example
Starting aprs2influxdb assuming an influxdb server is running and has a "mydb" database configured is simple. Please note that APRS-IS ignores logins from "nocall" so you will connect but likely see nothing if you do not specify your amateur radio callsign.

`aprs2influxdb --dbuser influxuser --dbpassword password123 --dbname mydb --callsign nocall`

The above command uses default values for the options not specified. APRS-IS port 10152 is the full stream while other ports exist this is the most useful. aprslib defaults to `rotate.aprs.net` to pick an APRS core server. Please see [APRS-IS Servers](http://www.aprs-is.net/aprsservers.aspx) for more information.
## Running the tests

Unit testing will be implemented in a future pull request

## Deployment
This has been tested on a Debian 9 (Stretch) server as well as locally with Windows 7 during development.

## Authors
* **Bryce Salmi** - *Initial work* - [KB1LQC](https://github.com/kb1lqc)

See also the list of [contributors](https://github.com/FaradayRF/aprs2influxdb/contributors) who participated in this project.

## Acknowledgments

* [@PhirePhly](https://github.com/PhirePhly) for answering my APRS questions!
* [@hessu](https://github.com/hessu) for also answering my APRS and aprsc questions as well as providing the awesome [aprs.fi](https://www.aprs.fi) website.
