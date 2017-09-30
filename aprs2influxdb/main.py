import aprslib
import ConfigParser
import influxdb
from influxdb import InfluxDBClient
import logging
import argparse
import os
import sys
import threading
import time
import shutil

# Globals
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aprs2influxdb")

# Command line input
parser = argparse.ArgumentParser(description='Connects to APRS-IS and saves stream to local InfluxDB')
parser.add_argument('--dbhost', help='Set InfluxDB host', default="localhost")
parser.add_argument('--dbport', help='Set InfluxDB port', default="8086")
parser.add_argument('--dbuser', help='Set InfluxDB user', default="root")
parser.add_argument('--dbpassword', help='Set InfluxDB password', default="root")
parser.add_argument('--dbname', help='Set InfluxDB database name', default="mydb")
parser.add_argument('--callsign', help='Set APRS-IS login callsign', default="nocall")
parser.add_argument('--port', help='Set APRS-IS port', default="10152")
parser.add_argument('--interval', help='Set APRS-IS heartbeat interval in minutes', default="15")

# Parse the arguments
args = parser.parse_args()

def editConfig(config, args):
    #Use command line values to change configuration
    if args.dbhost:
        config[0].set('influx', 'dbhost', args.dbhost)
    if args.dbport:
        config[0].set('influx', 'dbport', args.dbport)
    if args.dbuser:
        config[0].set('influx', 'dbuser', args.dbuser)
    if args.dbpassword:
        config[0].set('influx', 'dbpassword', args.dbpassword)
    if args.dbname:
        config[0].set('influx', 'dbname', args.dbname)
    if args.callsign:
        config[0].set('aprsis', 'callsign', args.callsign)
    if args.port:
        config[0].set('aprsis', 'port', args.port)
    if args.interval:
        config[0].set('aprsis', 'interval', args.interval)

    with open(config[1], 'wb') as configfile:
        config[0].write(configfile)


def getConfig():
        """
        Get configuration file
        """
        # Known paths where loggingConfig.ini can exist
        installPath = os.path.join(sys.prefix,"etc", "aprs2influxdb", "config.ini")
        localPath = os.path.join(os.curdir, "config.ini")


        # Check all directories until first instance of loggingConfig.ini
        for location in localPath, installPath:
            try:
                config = ConfigParser.RawConfigParser()
                result = config.read(location)
            except ConfigParser.NoSectionError:
                pass

            if result:
                break

        return [config, location]


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
            tags.append("format={0}".format(jsonData.get("format")))

        except KeyError as e:
            logger.error(e)
            logger.error(jsonData)

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

        try:
            comment = jsonData.get("comment").encode('ascii', 'ignore')

            if comment:
                logger.debug(comment)
                fields.append("comment=\"{0}\"".format(comment.replace("\"", "")))

        except UnicodeError as e:
            logger.error(e)

        except TypeError as e:
            logger.error(e)

        fieldsStr = ",".join(fields)

        return measurement + "," + tagStr + " " + fieldsStr


def callback(packet):
    logger.debug(packet)


    # Open a new connection every time, probably SLOWWWW
    influxConn = connectInfluxDB()
    line = jsonToLineProtocol(packet)

    if line:
        logger.debug(line)
        try:
            influxConn.write_points([line], protocol='line')

        except StandardError as e:
            logger.error(e)
            logger.error(packet)

        except influxdb.exceptions.InfluxDBClientError as e:
            logger.error(e)
            logger.error(packet)


def connectInfluxDB():
    config = getConfig()
    configFile = config[0]
    host = configFile.get('influx', 'dbhost')
    port = configFile.get('influx', 'dbport')
    user = configFile.get('influx', 'dbuser')
    password = configFile.get('influx', 'dbpassword')
    dbname = configFile.get('influx', 'dbname')

    return InfluxDBClient(host, port, user, password, dbname)

def consumer(conn):
    logger.debug("starting consumer thread")
    # Obtain raw APRS-IS packets and sent to callback when received
    conn.consumer(callback, immortal=True, raw=False)

def heartbeat(conn, callsign, interval):
    logger.debug("Starting heartbeat thread")
    while True:
        # Create timestamp
        timestamp = int(time.time())

        # Create APRS status message
        status = "{0}>APRS,TCPIP*:>aprs2influxdb heartbeat {1}"
        conn.sendall(status.format(callsign, timestamp))
        logger.debug("Sent heartbeat")

        # Sleep for specified time
        logger.info(interval)
        time.sleep(interval*60)  # Sent every interval minutes

def main():

    # Open up configuration file
    config = getConfig()

    # Edit configuration with user input
    editConfig(config, args)

    # Obtain APRS-IS configuration
    aprsCallsign = config[0].get('aprsis', 'callsign').upper()
    aprsPort = config[0].get('aprsis', 'port')
    passcode = aprslib.passcode(aprsCallsign)
    aprsInterval = config[0].getint('aprsis', 'interval')

    # Open APRS-IS connection
    AIS = aprslib.IS(aprsCallsign, passwd=passcode, port=aprsPort)
    AIS.connect()

    # Create heartbeat
    t1 = threading.Thread(target=heartbeat, args=(AIS, aprsCallsign, aprsInterval))

    # Create consumer
    t2 = threading.Thread(target=consumer, args=(AIS,))

    # Start threads
    t1.start()
    t2.start()


if __name__ == "__main__":
    main()
