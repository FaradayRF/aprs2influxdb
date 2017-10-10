import aprslib
import influxdb
from influxdb import InfluxDBClient
import logging
import argparse
import sys
import threading
import time
import os

from logging.handlers import TimedRotatingFileHandler

# Globals
#logging.basicConfig(level=logging.INFO)
#logger = logging.getLogger("aprs2influxdb")

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
parser.add_argument('--debug', help='Set logging level to DEBUG', action="store_true")

# Parse the arguments
args = parser.parse_args()


def jsonToLineProtocol(jsonData):
    """Converts JSON APRS-IS packet to influxdb line protocol

    Takes in a JSON packet from aprslib (raw=false) and parses it into an
    influxdb line protocol compliant string to insert into database. Returns
    a valid line protocol string ready to be inserted into the database.

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """

    # Parse uncompressed format packets
    if jsonData["format"] == "uncompressed":
        # Parse uncompressed APRS packet
        return parseUncompressed(jsonData)

    if jsonData["format"] == "mic-e":
        # Parse Mice-E APRS packet
        return parseMicE(jsonData)

    if jsonData["format"] == "object":
        # Parse Object APRS packet
        return parseObject(jsonData)

    if jsonData["format"] == "compressed":
        # Parse Object APRS packet
        return parseCompressed(jsonData)

    if jsonData["format"] == "status":
        # Parse status APRS packet
        return parseStatus(jsonData)

    if jsonData["format"] == "wx":
        # Parse WX APRS packet
        return parseWX(jsonData)

    if jsonData["format"] == "beacon":
        # Parse WX APRS packet
        return parseBeacon(jsonData)

    if jsonData["format"] == "bulletin":
        # Parse Bulletin APRS packet
        return parseBulletin(jsonData)

    if jsonData["format"] == "message":
        # Parse Message APRS packet
        return parseMessage(jsonData)

    # Uncomment for all other formats not yes parsed
    #logger.warning(jsonData["format"])


def parseUncompressed(jsonData):
    """Parse uncompressed APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    # Converts aprslib JSON to influxdb line protocol
    # Schema
    # measurement = packet
    # tag = from*
    # tag = to*
    # tag = symbolTable
    # tag = symbol
    # tag = format*
    # tag = objectFormat
    # tag = objectName
    # tag = via
    # tag = messageCapable
    # field = timestamp
    # field = rawTimestamp
    # field = latitude*
    # field = longitude*
    # field = posAmbiguity*
    # field = altitude*
    # field = speed
    # field = course
    # field = seq*
    # field = analog1*
    # field = analog2*
    # field = analog3*
    # field = analog4*
    # field = analog5*
    # field = bits*
    # field = comment*
    # field = path*
    # field = mbits
    # field = mtype
    # field = pressure
    # field = rain1H
    # field = rain24h
    # field = rainSinceMidnight
    # field = temperature
    # field = windDirection
    # field = windGust
    # field = windSpeed
    # field = wxRawTimestamp
    # field = status
    # field = addresse
    # field = messageText

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

    tagStr = ",".join(tags)

    try:
        fields.append("latitude={0}".format(jsonData.get("latitude", 0)))
        fields.append("longitude={0}".format(jsonData.get("longitude", 0)))
        fields.append("posAmbiguity={0}".format(jsonData.get("posambiguity", 0)))
        fields.append("altitude={0}".format(jsonData.get("altitude", 0)))
        fields.append("speed={0}".format(jsonData.get("speed", 0)))
        if jsonData.get("path"):
            fields.append(parsePath(jsonData.get("path")))

    except KeyError as e:
        logger.error(e)

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
        pass

    try:
        if jsonData["comment"]:
            fields.append(parseComment(jsonData["comment"]))

    except KeyError:
        # Comment fields often are not present so just pass
        pass

    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseMicE(jsonData):
    """Parse Mic-e APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    # Converts aprslib JSON to influxdb line protocol
    # Schema
    # measurement = packet*
    # tag = from*
    # tag = to*
    # tag = symbolTable
    # tag = symbol
    # tag = format*
    # tag = via*
    # field = latitude*
    # field = longitude*
    # field = posAmbiguity*
    # field = altitude*
    # field = speed*
    # field = course*
    # field = comment*
    # field = path*
    # field = mbits*
    # field = mtype*

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    try:
        tags.append("from={0}".format(jsonData.get("from")))
        tags.append("to={0}".format(jsonData.get("to")))
        if jsonData.get("via"):
            tags.append("via={0}".format(jsonData.get("via")))
        tags.append("format={0}".format(jsonData.get("format")))

    except KeyError as e:
        logger.error(e)

    tagStr = ",".join(tags)

    try:
        fields.append("latitude={0}".format(jsonData.get("latitude", 0)))
        fields.append("longitude={0}".format(jsonData.get("longitude", 0)))
        fields.append("posAmbiguity={0}".format(jsonData.get("posambiguity", 0)))
        fields.append("altitude={0}".format(jsonData.get("altitude", 0)))
        fields.append("speed={0}".format(jsonData.get("speed", 0)))
        fields.append("course={0}".format(jsonData.get("course", 0)))
        fields.append("mbits={0}".format(jsonData.get("mbits", 0)))
        fields.append("mtype=\"{0}\"".format(jsonData.get("mtype", 0)))
        if jsonData.get("path"):
            fields.append(parsePath(jsonData.get("path")))

    except KeyError as e:
        logger.error("KeyError: {0}, Mic-E Packet".format(e))
        logger.error(jsonData)

    try:
        if jsonData["comment"]:
            fields.append(parseComment(jsonData["comment"]))

    except KeyError:
        # Comment fields often are not present so just pass
        pass

    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseObject(jsonData):
    """Parse Object APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    # Converts aprslib JSON to influxdb line protocol
    # Schema
    # measurement = packet*
    # tag = from*
    # tag = to
    # tag = symbolTable
    # tag = symbol
    # tag = format*
    # tag = via
    # tag = alive*
    # field = objectFormat*
    # field = objectName*
    # field = latitude*
    # field = longitude*
    # field = posAmbiguity*
    # field = rawTimestamp*
    # field = timestamp*
    # field = speed*
    # field = course*
    # field = comment*
    # field = path*

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    try:
        tags.append("from={0}".format(jsonData.get("from")))
        tags.append("to={0}".format(jsonData.get("to")))
        if jsonData.get("via"):
            tags.append("via={0}".format(jsonData.get("via")))
        tags.append("format={0}".format(jsonData.get("format")))
        tags.append("alive={0}".format(jsonData.get("alive")))

    except KeyError as e:
        logger.error(e)

    tagStr = ",".join(tags)

    try:
        fields.append("latitude={0}".format(jsonData.get("latitude", 0)))
        fields.append("longitude={0}".format(jsonData.get("longitude", 0)))
        fields.append("posAmbiguity={0}".format(jsonData.get("posambiguity", 0)))
        fields.append("speed={0}".format(jsonData.get("speed", 0)))
        fields.append("course={0}".format(jsonData.get("course", 0)))
        fields.append("rawTimestamp=\"{0}\"".format(jsonData.get("raw_timestamp", 0)))
        fields.append("timestamp={0}".format(jsonData.get("timestamp", 0)))
        fields.append("objectFormat=\"{0}\"".format(jsonData.get("object_format")))
        fields.append("objectName=\"{0}\"".format(jsonData.get("object_name")))
        if jsonData.get("path"):
            fields.append(parsePath(jsonData.get("path")))

    except KeyError as e:
        logger.error("KeyError: {0}, Object Packet".format(e))
        logger.error(jsonData)

    try:
        if jsonData["comment"]:
            fields.append(parseComment(jsonData["comment"]))

    except KeyError:
        # Comment fields often are not present so just pass
        pass

    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseStatus(jsonData):
    """Parse Status APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    # Converts aprslib JSON to influxdb line protocol
    # Schema
    # measurement = packet*
    # tag = from*
    # tag = to*
    # tag = format*
    # tag = via*
    # field = status*
    # field = path*

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    try:
        tags.append("from={0}".format(jsonData.get("from")))
        tags.append("to={0}".format(jsonData.get("to")))
        if jsonData.get("via"):
            tags.append("via={0}".format(jsonData.get("via")))
        tags.append("format={0}".format(jsonData.get("format")))

    except KeyError as e:
        logger.error(e)

    tagStr = ",".join(tags)

    fields.append(parseStatusValue(jsonData["status"]))
    if jsonData.get("path"):
        fields.append(parsePath(jsonData.get("path")))

    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseCompressed(jsonData):
    """Parse Compressed APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    # Converts aprslib JSON to influxdb line protocol
    # Schema
    # measurement = packet
    # tag = from*
    # tag = to*
    # tag = symbolTable
    # tag = symbol
    # tag = format*
    # tag = via*
    # tag = messageCapable*
    # field = latitude*
    # field = longitude*
    # field = gpsFixStatus*
    # field = altitude*
    # field = seq*
    # field = analog1*
    # field = analog2*
    # field = analog3*
    # field = analog4*
    # field = analog5*
    # field = bits*
    # field = comment*
    # field = path*

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    try:
        tags.append("from={0}".format(jsonData.get("from")))
        tags.append("to={0}".format(jsonData.get("to")))
        tags.append("format={0}".format(jsonData.get("format")))
        tags.append("messageCapable={0}".format(jsonData.get("messagecapable")))

    except KeyError as e:
        logger.error(e)

    tagStr = ",".join(tags)

    try:
        fields.append("latitude={0}".format(jsonData.get("latitude", 0)))
        fields.append("longitude={0}".format(jsonData.get("longitude", 0)))
        fields.append("altitude={0}".format(jsonData.get("altitude", 0)))
        fields.append("gpsFixStatus={0}".format(jsonData.get("gpsfixstatus", 0)))
        if jsonData.get("path"):
            fields.append(parsePath(jsonData.get("path")))

    except KeyError as e:
        logger.error(e)

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
        pass

    try:
        if jsonData["comment"]:
            fields.append(parseComment(jsonData["comment"]))

    except KeyError:
        # Comment fields often are not present so just pass
        pass

    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseWX(jsonData):
    """Parse WX APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    # Converts aprslib JSON to influxdb line protocol
    # Schema
    # measurement = packet*
    # tag = from
    # tag = to
    # tag = format
    # tag = via
    # field = wxRawTimestamp
    # field = comment
    # field = pressure
    # field = rain1h
    # field = rain24h
    # field = rainSinceMidnight
    # field = temperature
    # field = windDirection
    # field = windGust
    # field = windSpeed
    # field = path*

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    try:
        tags.append("from={0}".format(jsonData.get("from")))
        tags.append("to={0}".format(jsonData.get("to")))
        if jsonData.get("via"):
            tags.append("via={0}".format(jsonData.get("via")))
        tags.append("format={0}".format(jsonData.get("format")))

    except KeyError as e:
        logger.error(e)

    tagStr = ",".join(tags)

    try:
        fields.append("wxRawTimestamp={0}".format(jsonData.get("wx_raw_timestamp", 0)))
        fields.append("pressure={0}".format(jsonData.get("pressure", 0)))
        fields.append("rain1h={0}".format(jsonData.get("rain_1h", 0)))
        fields.append("rain24h={0}".format(jsonData.get("rain_24h", 0)))
        fields.append("rainSinceMidnight={0}".format(jsonData.get("rain_since_midnight", 0)))
        fields.append("temperature={0}".format(jsonData.get("temperature", 0)))
        fields.append("windDirection={0}".format(jsonData.get("wind_direction", 0)))
        fields.append("windGust={0}".format(jsonData.get("wind_gust", 0)))
        fields.append("windSpeed={0}".format(jsonData.get("wind_speed", 0)))
        if jsonData.get("path"):
            fields.append(parsePath(jsonData.get("path")))

    except KeyError as e:
        logger.error("KeyError: {0}, Object Packet".format(e))
        logger.error(jsonData)

    try:
        if jsonData["comment"]:
            fields.append(parseComment(jsonData["comment"]))

    except KeyError:
        # Comment fields often are not present so just pass
        pass

    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseBeacon(jsonData):
    """Parse Beacon APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    # Converts aprslib JSON to influxdb line protocol
    # Schema
    # measurement = packet
    # tag = from
    # tag = to
    # tag = format
    # tag = via
    # field = text*
    # field = path*

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    try:
        tags.append("from={0}".format(jsonData.get("from")))
        tags.append("to={0}".format(jsonData.get("to")))
        if jsonData.get("via"):
            tags.append("via={0}".format(jsonData.get("via")))
        tags.append("format={0}".format(jsonData.get("format")))

    except KeyError as e:
        logger.error(e)

    tagStr = ",".join(tags)

    fields.append(parseTextValue(jsonData["text"]))
    if jsonData.get("path"):
        fields.append(parsePath(jsonData.get("path")))

    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseBulletin(jsonData):
    """Parse Bulletin APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    # Converts aprslib JSON to influxdb line protocol
    # Schema
    # measurement = packet
    # tag = from
    # tag = to
    # tag = format
    # tag = via
    # field = messageText
    # field = bid
    # field = identifier (empty)
    # field = path*

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    try:
        tags.append("from={0}".format(jsonData.get("from")))
        tags.append("to={0}".format(jsonData.get("to")))
        if jsonData.get("via"):
            tags.append("via={0}".format(jsonData.get("via")))
        tags.append("format={0}".format(jsonData.get("format")))

    except KeyError as e:
        logger.error(e)

    tagStr = ",".join(tags)

    try:
        if jsonData["message_text"]:
            fields.append(parseMessageText(jsonData["message_text"]))

    except KeyError:
        # happens
        pass

    fields.append("bid={0}".format(jsonData.get("bid", 0)))
    if jsonData.get("identifier"):
        fields.append("identifier={0}".format(jsonData.get("identifier")))
    if jsonData.get("path"):
        fields.append(parsePath(jsonData.get("path")))

    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseMessage(jsonData):
    """Parse Message APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    # Converts aprslib JSON to influxdb line protocol
    # Schema
    # measurement = packet
    # tag = from
    # tag = to
    # tag = format
    # tag = via
    # tag = addresse
    # field = messageText
    # field = bid
    # field = identifier (empty)
    # field = path*

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    try:
        tags.append("from={0}".format(jsonData.get("from")))
        tags.append("to={0}".format(jsonData.get("to")))
        if jsonData.get("via"):
            tags.append("via={0}".format(jsonData.get("via")))
        tags.append("format={0}".format(jsonData.get("format")))
        tags.append("addresse={0}".format(jsonData.get("addresse")))

    except KeyError as e:
        logger.error(e)

    tagStr = ",".join(tags)

    try:
        if jsonData["message_text"]:
            fields.append(parseMessageText(jsonData["message_text"]))

    except KeyError:
        # happens
        pass

    fields.append("bid={0}".format(jsonData.get("bid", 0)))
    if jsonData.get("identifier"):
        fields.append("identifier={0}".format(jsonData.get("identifier")))
    if jsonData.get("path"):
        fields.append(parsePath(jsonData.get("path")))

    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseComment(rawComment):
    try:
        comment = rawComment.encode('ascii', 'ignore')
        commentStr = ("comment=\"{0}\"".format(comment.replace("\"", "")))

    except UnicodeError as e:
        logger.error(e)

    except TypeError as e:
        logger.error(e)

    return commentStr


def parseStatusValue(rawStatus):
    try:
        status = rawStatus.encode('ascii', 'ignore')
        statusStr = ("status=\"{0}\"".format(status.replace("\"", "")))

    except UnicodeError as e:
        logger.error(e)

    except TypeError as e:
        logger.error(e)

    return statusStr


def parseTextValue(rawText):
    try:
        text = rawText.encode('ascii', 'ignore')
        textStr = ("text=\"{0}\"".format(text.replace("\"", "")))

    except UnicodeError as e:
        logger.error(e)

    except TypeError as e:
        logger.error(e)

    return textStr


def parseMessageText(rawText):
    try:
        text = rawText.encode('ascii', 'ignore')
        textStr = ("messageText=\"{0}\"".format(text.replace("\"", "")))

    except UnicodeError as e:
        logger.error("UnicodeError: {0}".format(rawText))
        logger.error(e)

    except TypeError as e:
        logger.error("TypeError: {0}".format(rawText))
        logger.error(e)

    return textStr


def parsePath(path):
    """Take path and turn into a string

    keyword arguments:
    path -- list of paths
    """

    temp = ",".join(path)
    pathStr = ("path=\"{0}\"".format(temp))

    return pathStr


def callback(packet):
    """aprslib callback for every packet received from APRS-IS connection

    keyword arguments:
    packet -- APRS-IS packet from aprslib connection
    """
    #logger.info(packet)

    # Open a new connection every time, probably SLOWWWW
    influxConn = connectInfluxDB()
    try:
        line = jsonToLineProtocol(packet)
    except StandardError as e:
        logger.error(e)

    if line:
        #logger.debug(line)
        try:
            influxConn.write_points([line], protocol='line')

        except StandardError as e:
            logger.error(e)
            logger.error(packet)

        except influxdb.exceptions.InfluxDBClientError as e:
            logger.error(e)
            logger.error(packet)


def connectInfluxDB():
    """Connect to influxdb database with configuration values"""

    return InfluxDBClient(args.dbhost,
                          args.dbport,
                          args.dbuser,
                          args.dbpassword,
                          args.dbname)


def consumer(conn):
    """Start consumer function for thread

    keyword arguments:
    conn -- APRS-IS connection from aprslib
    """
    logger.debug("starting consumer thread")
    # Obtain raw APRS-IS packets and sent to callback when received
    conn.consumer(callback, immortal=True, raw=False)


def heartbeat(conn, callsign, interval):
    """Send out an APRS status message to keep connection alive

    keyword arguments:
    conn -- APRS-IS connction from aprslib
    callsign -- Callsign of status message
    interval -- Minutes betwee status messages
    """
    logger.debug("Starting heartbeat thread")
    while True:
        # Create timestamp
        timestamp = int(time.time())

        # Create APRS status message
        status = "{0}>APRS,TCPIP*:>aprs2influxdb heartbeat {1}"
        conn.sendall(status.format(callsign, timestamp))
        logger.debug("Sent heartbeat")

        # Sleep for specified time
        time.sleep(float(interval) * 60)  # Sent every interval minutes


def createLog(path, debug=False):
    """Create a rotating log at the specified path and return logger

    keyword arguments:
    path -- path to log file
    debug -- Boolean to set DEBUG log level,
    """
    tempLogger = logging.getLogger(__name__)

    # Add handler for rotating file
    handler = TimedRotatingFileHandler(path,
                                       when="h",
                                       interval=1,
                                       backupCount=5)
    tempLogger.addHandler(handler)

    # Add handler for stdout printing
    screenHandler = logging.StreamHandler(sys.stdout)
    tempLogger.addHandler(screenHandler)

    # Set logging level
    if debug:
        tempLogger.setLevel(logging.DEBUG)
    else:
        tempLogger.setLevel(logging.WARNING)

    return tempLogger


def main():
    """Main function of aprs2influxdb

    Reads in configuration values and starts connection to APRS-IS with aprslib.
    Then two threads are started, one to monitor for APRS-IS packets and
    another to periodically send status packets to APRS-IS in order to keep
    the connection alive.
    """
    # Create logger, must be global for functions and threads
    global logger

    # Log to sys.prefix + aprs2influxdb.log
    log = os.path.join(sys.prefix, "aprs2influxdb.log")
    logger = createLog(log, args.debug)

    # Start login for APRS-IS
    logger.info("Logging into APRS-IS as {0} on port {1}".format(args.callsign, args.port))
    if args.callsign == "nocall":
        logger.warning("APRS-IS ignores the callsign \"nocall\"!")

    # Open APRS-IS connection
    passcode = aprslib.passcode(args.callsign)
    AIS = aprslib.IS(args.callsign,
                     passwd=passcode,
                     port=args.port)

    AIS.logger = logger
    try:
        AIS.connect()

    except aprslib.exceptions.LoginError as e:
        logger.error(e)
        logger.info("APRS Login Callsign: {0} Port: {1}".format(args.callsign, args.port))
        sys.exit(1)

    except aprslib.exceptions.ConnectionError as e:
        logger.error(e)
        sys.exit(1)

    # Create heartbeat
    t1 = threading.Thread(target=heartbeat, args=(AIS, args.callsign, args.interval))

    # Create consumer
    t2 = threading.Thread(target=consumer, args=(AIS,))

    # Start threads
    t1.start()
    t2.start()


if __name__ == "__main__":
    main()
