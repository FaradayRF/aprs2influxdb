import aprslib
import ConfigParser
import influxdb
from influxdb import InfluxDBClient
import logging
import argparse
import os

# Globals
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aprs2influxdb")

# Command line input
parser = argparse.ArgumentParser(description='Connects to APRS-IS and saves stream to local InfluxDB')
parser.add_argument('--init-config', dest='init', action='store_true', help='Initialize configuration file')
parser.add_argument('--host', help='Set InfluxDB host')
parser.add_argument('--port', help='Set InfluxDB port')
parser.add_argument('--user', help='Set InfluxDB user')
parser.add_argument('--password', help='Set InfluxDB password')
parser.add_argument('--dbname', help='Set InfluxDB database name')
parser.add_argument('--dbuser', help='Set InfluxDB user')
parser.add_argument('--dbuserpassword', help='Set InfluxDB user password')

# Parse the arguments
args = parser.parse_args()

logger.info("woot")

def getConfig():
        """
        Get configuration file
        """
        # Known paths where loggingConfig.ini can exist
        relpath1 = os.path.join('etc', 'aprs2influxdb')

        # Check all directories until first instance of loggingConfig.ini
        for location in os.curdir, relpath1:
            try:
                config = ConfigParser.RawConfigParser()
                result = config.read(os.path.join(location, "config.ini"))
            except ConfigParser.NoSectionError:
                pass

            if result:
                break

        return config

def jsonToLineProtocol(jsonData):
    # Converts aprslib JSON to influxdb line protocol
    # Schema
    # measurement = packet
    # tag = from
    # tag = to
    # tag = symbolTable
    # tag = symbol
    # tag = format
    # tag = comment
    # field = latitude
    # field = longitude
    # field = posAmbiguity
    # field = altitude
    # field = speed
    # field = sequenceNumber
    # field = analog1
    # field = analog2
    # field = analog3
    # field = analog4
    # field = analog5
    # field = digital

    if jsonData["format"] == "uncompressed":
        # initialize variables
        tags = []
        fields = []

        # Set measurement to "packet"
        measurement = "packet"

        try:
            tags.append("from={0}".format(jsonData.get("from")))
            tags.append("to={0}".format(jsonData.get("to")))
            #symbolTable = jsonData.get("symbol_table")
            #tags.append("symbolTable=\"{0}\"".format(symbolTable.encode("ascii", errors="replace")))
            #symbol = jsonData.get("symbol")
            #tags.append("symbol=\"{0}\"".format(symbol.encode("ascii", errors="replace")))
            tags.append("format={0}".format(jsonData.get("format")))

        except KeyError as e:
            logger.error(e)
            logger.error(jsonData)

        # try:
        #     rawComment = jsonData.get("comment")
        #     comment = rawComment.encode("ascii", errors="ignore")
        #     tags.append("comment=\"{0}\"".format(comment))
        #
        # except KeyError as e:
        #     logger.error(e)
        #     logger.error(jsonData)
        #
        # except UnicodeError as e:
        #     logger.error(e)
        #     logger.error(jsonData)

        tagStr = ",".join(tags)

        try:
            fields.append("latitude={0}".format(jsonData.get("latitude", 0)))
            fields.append("longitude={0}".format(jsonData.get("longitude", 0)))
            fields.append("posAmbiguity={0}".format(jsonData.get("posambiguity", 0)))
            fields.append("altitude={0}".format(jsonData.get("altitude", 0)))
            fields.append("speed={0}".format(jsonData.get("speed", 0)))
        except KeyError as e:
            logger.error(e)
            logger.error(jsonData)

        try:
            if jsonData["telemetry"]["seq"]:
                fields.append("sequenceNumber={0}".format(jsonData["telemetry"]["seq"]))
                fields.append("analog1={0}".format(jsonData["telemetry"]["vals"][0]))
                fields.append("analog2={0}".format(jsonData["telemetry"]["vals"][1]))
                fields.append("analog3={0}".format(jsonData["telemetry"]["vals"][2]))
                fields.append("analog4={0}".format(jsonData["telemetry"]["vals"][3]))
                fields.append("analog5={0}".format(jsonData["telemetry"]["vals"][4]))
                fields.append("digital={0}".format(jsonData["telemetry"]["bits"]))

        except KeyError as e:
            # Expect many KeyErrors for stations not sending telemetry
            logger.debug(e)
            logger.debug(jsonData)

        fieldsStr = ",".join(fields)

        return measurement + "," + tagStr + " " + fieldsStr


def callback(packet):
    packet = aprslib.parse(packet)
    # Open a new connection every time, probably slow
    influxConn = connectInfluxDB()
    line = jsonToLineProtocol(packet)

    if line:
        logger.debug(line)
        try:
            influxConn.write_points([line], protocol='line')

        except StandardError as e:
            logger.error(e)
            #logger.error(jsonData)

        except influxdb.exceptions.InfluxDBClientError as e:
            logger.error(e)
            #logger.error(jsonData)


def connectInfluxDB():
    configFile = getConfig()
    host = configFile.get('influx','host')
    port = configFile.get('influx','port')
    user = configFile.get('influx','user')
    password = configFile.get('influx','password')
    dbname = configFile.get('influx','dbname')
    #dbuser = config['influx']['dbuser']
    #dbuser_password = config['influx']['dbuserpassword']

    return InfluxDBClient(host, port, user, password, dbname)


def main():
    # Start logger


    # Open APRS-IS connection
    AIS = aprslib.IS("KB1LQC")
    AIS.connect()

    # Obtain raw APRS-IS packets and sent to callback when received
    AIS.consumer(callback, raw=True)


if __name__ == "__main__":
    main()
