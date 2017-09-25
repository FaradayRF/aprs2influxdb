import aprslib
import configparser
import influxdb
from influxdb import InfluxDBClient
import Geohash
import time
import logging

# Globals
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DrWatson")

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

        try:
	    rawComment = jsonData.get("comment")
	    comment = rawComment.encode("ascii", errors="ignore")
	    #tags.append("comment=\"{0}\"".format(comment))

        except KeyError as e:
            logger.error(e)
            logger.error(jsonData)

        except UnicodeError as e:
            logger.error(e)
            logger.error(jsonData)

        tagStr = ",".join(tags)

        try:
            fields.append("latitude={0}".format(jsonData.get("latitude", 0)))
            fields.append("longitude={0}".format(jsonData.get("longitude", 0)))
            fields.append("posAmbiguity={0}".format(jsonData.get("posambiguity", 0)))
            fields.append("altitude={0}".format(jsonData.get("altitude",0)))
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

    config = configparser.ConfigParser()
    config.read('clerk.ini')

    host = config['influx']['host']
    port = config['influx']['port']
    user = config['influx']['user']
    password = config['influx']['password']
    dbname = config['influx']['dbname']
    dbuser = config['influx']['dbuser']
    dbuser_password = config['influx']['dbuserpassword']

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
