import aprslib
import influxdb
from influxdb import InfluxDBClient
import logging
import argparse
import sys
import threading
import time
import os
import math

from logging.handlers import TimedRotatingFileHandler

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

    try:
        if jsonData["format"] == "uncompressed":
            # Parse uncompressed APRS packet
            return parseUncompressed(jsonData)

        if jsonData["format"] == "mic-e":
            # Parse mic-e APRS packet
            return parseMicE(jsonData)

        if jsonData["format"] == "object":
            # Parse object APRS packet
            return parseObject(jsonData)

        if jsonData["format"] == "compressed":
            # Parse compressed APRS packet
            return parseCompressed(jsonData)

        if jsonData["format"] == "status":
            # Parse status APRS packet
            return parseStatus(jsonData)

        if jsonData["format"] == "wx":
            # Parse wx APRS packet
            return parseWX(jsonData)

        if jsonData["format"] == "beacon":
            # Parse beacon APRS packet
            return parseBeacon(jsonData)

        if jsonData["format"] == "bulletin":
            # Parse bulletin APRS packet
            return parseBulletin(jsonData)

        if jsonData["format"] == "message":
            # Parse message APRS packet
            return parseMessage(jsonData)

        if jsonData["format"] == "telemetry-message":
            # Parse telemetry-message APRS packet
            # Currently only support scaling values
            return parseTelemetryScaling(jsonData)

        # All other formats not yes parsed
        logger.debug("Not parsing {0} packets".format(jsonData))

    except StandardError:
        # An error occured
        logger.error('A parsing StandardError occured', exc_info=True)
        logger.error("Packet: {0}".format(jsonData))


def parseTelemetry(jsonData, fieldList):
    '''parse telemetry from packets

    Iterates through a packet to extra telemetry data: sequence, bits, and
    values. These are placed into the fieldList which is returned at the end of
    the function.

    keyword arguments:
    jsonData -- JSON packet from aprslib
    fieldList -- list of field items currently parsed
    '''

    # Check for telemetry in packet
    if "telemetry" in jsonData:
        items = jsonData.get("telemetry")
        # Extract telemetry sequency
        if "seq" in items:
            fieldList.append("seq={0}".format(items.get("seq")))
        # Extract IO bits
        if "bits" in items:
            fieldList.append("bits={0}".format(items.get("bits")))
        # Attempt to retrieve scaling values from telemetryDictionary
        try:
            channels = telemetryDictionary[jsonData["from"]]
        except KeyError as e:
            # No scaling values found, assign generic scaling to channels
            channels = []
            for eqn in range(5):
                # Create a scaling dictionary for all five measurements
                equations = {"a": 0, "b": 0, "c": 0, }
                equations["a"] = 0
                equations["b"] = 1
                equations["c"] = 0
                channels.append(equations)

        # Extract analog values from telemtry packet
        if "vals" in items:
            values = items.get("vals")
            for analog in range(5):
                # Apply scaling equation A*V**2 + B*V + C
                telemVal = channels[analog]["a"]*math.pow(values[analog],2) + channels[analog]["b"]*values[analog] + channels[analog]["c"]
                fieldList.append("analog{0}={1}".format(analog + 1, telemVal))

    # Return fieldList with found items appended
    return fieldList


def parseEquations(jsonData):
    '''
    Iterates through a telemetry-message packet for tEQNs values which are
    scaling parameters for telemetry data. Places each equation coefficient into
    a dictionary which is then placed into a list for each measurement.
    Returns a channels list or None

    keyword arguments:
    jsonData -- JSON packet from aprslib
    '''
    # Check for tEQNS dictionary
    if("tEQNS" in jsonData):
        # Exists, initialize channels list and extract equations list
        channels = []
        items = jsonData.get("tEQNS")
        for eqn in items:
            # Iterate through each measurement coefficient list, assign to dictionary
            equations = {"a": 0, "b": 0, "c": 0, }
            equations["a"] = eqn[0]
            equations["b"] = eqn[1]
            equations["c"] = eqn[2]
            channels.append(equations)
        return channels
    return None


def parseWeather(jsonData, fieldList):
    '''parse weather data from packets

    Iterates through a packet to extra weather data. Items which are found are
    appended to the fieldList which is returned.

    keyword arguments:
    jsonData -- JSON packet from aprslib
    fieldList -- list of field items currently parsed
    '''

    # Check for weather data key
    if "weather" in jsonData:
        items = jsonData.get("weather")

        # Define weather items to check for
        wxFields = ["humidity", "pressure", "rain_1h", "rain_24h", "rain_since_midnight", "temperature", "wind_direction", "wind_gust", "wind_speed"]
        for key in wxFields:
            if key in items:
                fieldList.append("{0}={1}".format(key, items.get(key)))

    # Return fieldList with found items appended
    return fieldList


def parseUncompressed(jsonData):
    """Parse uncompressed APRS packets into influxedb line protocol. Returns a
    valid line protocol string.

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    ## Schema
    # field = from
    # field = to
    # field = symbol_table
    # field = symbol
    # tag = format
    # field = via
    # field = messagecapable
    # field = latitude
    # field = longitude
    # field = posAmbiguity
    # field = altitude
    # field = raw
    # field = speed
    # field = course
    # field = raw_timestamp
    # field = seq
    # field = analog1
    # field = analog2
    # field = analog3
    # field = analog4
    # field = analog5
    # field = bits
    # field = phg
    # field = rng
    # field = comment
    # field = path
    # field = pressure
    # field = rain_1h
    # field = rain_24h
    # field = rain_since_midnight
    # field = temperature
    # field = wind_direction
    # field = wind_gust
    # field = wind_speed

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    # Obtain tags
    #
    tags.append("format={0}".format(jsonData.get("format")))

    # Join tags into comma separated string
    tagStr = ",".join(tags)

    # Create field key lists to iterate through
    fieldNumKeys = ["latitude", "longitude", "posambiguity", "altitude", "speed", "course"]
    fieldTextKeys = ["from", "to", "messagecapable", "phg", "rng", "via"]

    # Extract number fields from packet
    for key in fieldNumKeys:
        if key in jsonData:
            fields.append("{0}={1}".format(key, jsonData.get(key)))

    # Extract text fields from packet
    for key in fieldTextKeys:
        if key in jsonData:
            fields.append("{0}=\"{1}\"".format(key, jsonData.get(key)))

    # Extract path
    if "path" in jsonData:
        fields.append(parsePath(jsonData.get("path")))

    # Extract comment
    if "comment" in jsonData:
        comment = parseTextString(jsonData.get("comment"), "comment")
        if len(jsonData.get("comment")) > 0:
            fields.append(comment)

    # Extract raw packet
    if "raw" in jsonData:
        comment = parseTextString(jsonData.get("raw"), "raw")
        if len(jsonData.get("raw")) > 0:
            fields.append(comment)

    # Extract APRS symbol
    if "symbol" in jsonData:
        comment = parseTextString(jsonData.get("symbol"), "symbol")
        if len(jsonData.get("symbol")) > 0:
            fields.append(comment)

    # Extract APRS symbol table
    if "symbol_table" in jsonData:
        comment = parseTextString(jsonData.get("symbol_table"), "symbol_table")
        if len(jsonData.get("symbol_table")) > 0:
            fields.append(comment)

    # Extract raw timestamp from packet
    if "raw_timestamp" in jsonData:
        rawtimestamp = parseTextString(jsonData.get("raw_timestamp"), "raw_timestamp")
        if len(jsonData.get("raw_timestamp")) > 0:
            fields.append(rawtimestamp)

    # Parse telemetry data
    fields = parseTelemetry(jsonData, fields)

    # Parse weather data
    fields = parseWeather(jsonData, fields)

    # Combine all fields into a valid line protocol string
    fieldsStr = ",".join(fields)

    # Combine final valid line protocol string
    return measurement + "," + tagStr + " " + fieldsStr


def parseMicE(jsonData):
    """Parse mic-e APRS packets into influxedb line protocol. Returns a
    valid line protocol string.

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    ## Schema
    # measurement = packet
    # field = from
    # field = symbol_table
    # field = symbol
    # tag = format
    # field = via
    # field = latitude
    # field = longitude
    # field = posambiguity
    # field = altitude
    # field = speed
    # field = course
    # field = comment
    # field = path
    # field = mbits
    # field = mtype
    # field = raw
    # field = to
    # field = daodatumbyte
    # field = path

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    # Obtain tags
    tags.append("format={0}".format(jsonData.get("format")))

    # Join tags into comma separated string
    tagStr = ",".join(tags)

    # Create field key lists to iterate through
    fieldNumKeys = ["latitude", "longitude", "posambiguity", "altitude", "speed", "course", "mbits"]
    fieldTextKeys = ["from", "via", "to", "mtype", "daodatumbyte"]

    # Extract number fields from packet
    for key in fieldNumKeys:
        if key in jsonData:
            fields.append("{0}={1}".format(key, jsonData.get(key)))

    # Extract text fields from packet
    for key in fieldTextKeys:
        if key in jsonData:
            fields.append("{0}=\"{1}\"".format(key, jsonData.get(key)))

    # Extract path
    if "path" in jsonData:
        fields.append(parsePath(jsonData.get("path")))

    # Extract comment
    if "comment" in jsonData:
        comment = parseTextString(jsonData.get("comment"), "comment")
        if len(jsonData.get("comment")) > 0:
            fields.append(comment)
        else:
            pass

    # Extract raw packet
    if "raw" in jsonData:
        comment = parseTextString(jsonData.get("raw"), "raw")
        if len(jsonData.get("raw")) > 0:
            fields.append(comment)

    # Extract APRS symbol
    if "symbol" in jsonData:
        comment = parseTextString(jsonData.get("symbol"), "symbol")
        if len(jsonData.get("symbol")) > 0:
            fields.append(comment)

    # Extract APRS symbol table
    if "symbol_table" in jsonData:
        comment = parseTextString(jsonData.get("symbol_table"), "symbol_table")
        if len(jsonData.get("symbol_table")) > 0:
            fields.append(comment)

    # Combine final valid line protocol string
    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseObject(jsonData):
    """Parse Object APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    # Converts aprslib JSON to influxdb line protocol
    # Schema
    # measurement = packet
    # field = from
    # field = to
    # field = symbol_table
    # field = symbol
    # tag = format
    # field = via
    # field = alive
    # field = object_format
    # field = object_name
    # field = latitude
    # field = longitude
    # field = posambiguity
    # field = raw_timestamp
    # field = timestamp
    # field = speed
    # field = course
    # field = altitude
    # field = comment
    # field = path
    # field  = raw
    # field = daodatumbyte
    # field = rng
    # field = bits
    # field = seq
    # field = analog1
    # field = analog2
    # field = analog3
    # field = analog4
    # field = analog5

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    # Obtain tags
    #tags.append("from={0}".format(jsonData.get("from")))
    tags.append("format={0}".format(jsonData.get("format")))

    # Join tags into comma separated string
    tagStr = ",".join(tags)

    # Create field key lists to iterate through
    fieldNumKeys = ["latitude", "longitude", "posambiguity", "speed", "course", "timestamp", "altitude"]
    fieldTextKeys = ["from", "alive", "via", "to", "object_format", "object_name", "rng", "daodatumbyte"]

    # Extract number fields from packet
    for key in fieldNumKeys:
        if key in jsonData:
            fields.append("{0}={1}".format(key, jsonData.get(key)))

    # Extract text fields from packet
    for key in fieldTextKeys:
        if key in jsonData:
            fields.append("{0}=\"{1}\"".format(key, jsonData.get(key)))

    # Extract path
    if "path" in jsonData:
        fields.append(parsePath(jsonData.get("path")))

    # Extract comment
    if "comment" in jsonData:
        comment = parseTextString(jsonData.get("comment"), "comment")
        if len(jsonData.get("comment")) > 0:
            fields.append(comment)

    # Parse telemetry
    fields = parseTelemetry(jsonData, fields)

    # Extract raw packet
    if "raw" in jsonData:
        comment = parseTextString(jsonData.get("raw"), "raw")
        if len(jsonData.get("raw")) > 0:
            fields.append(comment)

    # Extract symbol
    if "symbol" in jsonData:
        comment = parseTextString(jsonData.get("symbol"), "symbol")
        if len(jsonData.get("symbol")) > 0:
            fields.append(comment)

    # Extract symbol table
    if "symbol_table" in jsonData:
        comment = parseTextString(jsonData.get("symbol_table"), "symbol_table")
        if len(jsonData.get("symbol_table")) > 0:
            fields.append(comment)

    # Extract raw_timestamp
    if "raw_timestamp" in jsonData:
        rawtimestamp = parseTextString(jsonData.get("raw_timestamp"), "raw_timestamp")
        if len(jsonData.get("raw_timestamp")) > 0:
            fields.append(rawtimestamp)

    # Combine final valid line protocol string
    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseStatus(jsonData):
    """Parse Status APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    ## Schema
    # measurement = packet
    # field = from
    # field = to
    # tag = format
    # field = via
    # field = status
    # field = path
    # field = timestamp
    # field = raw
    # field = raw_timestamp

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    # Obtain tags
    tags.append("format={0}".format(jsonData.get("format")))

    # Join tags into comma separated string
    tagStr = ",".join(tags)

    # Create field key lists to iterate through
    fieldNumKeys = ["timestamp"]
    fieldTextKeys = ["from", "via", "to"]

    # Extract number fields from packet
    for key in fieldNumKeys:
        if key in jsonData:
            fields.append("{0}={1}".format(key, jsonData.get(key)))

    # Extract text fields from packet
    for key in fieldTextKeys:
        if key in jsonData:
            fields.append("{0}=\"{1}\"".format(key, jsonData.get(key)))

    # Extract path
    if "path" in jsonData:
        fields.append(parsePath(jsonData.get("path")))

    # Extract telemetry
    fields = parseTelemetry(jsonData, fields)

    # Extract status
    if "status" in jsonData:
        comment = parseTextString(jsonData.get("status"), "status")
        if len(jsonData.get("status")) > 0:
            fields.append(comment)

    # Extract raw packet
    if "raw" in jsonData:
        comment = parseTextString(jsonData.get("raw"), "raw")
        if len(jsonData.get("raw")) > 0:
            fields.append(comment)

    # Extract raw timestamp
    if "raw_timestamp" in jsonData:
        rawtimestamp = parseTextString(jsonData.get("raw_timestamp"), "raw_timestamp")
        if len(jsonData.get("raw_timestamp")) > 0:
            fields.append(rawtimestamp)

    # Combine final valid line protocol string
    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseCompressed(jsonData):
    """Parse Compressed APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    ## Schema
    # measurement = packet
    # field = from
    # field = to
    # field = symbol_table
    # field = symbol
    # tag = format
    # field = via
    # field = messagecapable
    # field = latitude
    # field = longitude
    # field = gpsfixstatus
    # field = altitude
    # field = seq
    # field = analog1
    # field = analog2
    # field = analog3
    # field = analog4
    # field = analog5
    # field = bits
    # field = comment
    # field = path
    # field = phg
    # field = raw
    # field = timestamp
    # field = pressure
    # field = rain_1h
    # field = rain_24h
    # field = rain_since_midnight
    # field = temperature
    # field = wind_direction
    # field = wind_gust
    # field = wind_speed
    # field = speed
    # field = course

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    # Obtain tags
    tags.append("format={0}".format(jsonData.get("format")))

    # Join tags into comma separated string
    tagStr = ",".join(tags)

    # Create field key lists to iterate through
    fieldNumKeys = ["latitude", "longitude", "gpsfixstatus", "altitude", "speed", "course", "timestamp"]
    fieldTextKeys = ["from", "to", "messagecapable", "phg", "via"]

    # Extract number fields from packet
    for key in fieldNumKeys:
        if key in jsonData:
            fields.append("{0}={1}".format(key, jsonData.get(key)))

    # Extract text fields from packet
    for key in fieldTextKeys:
        if key in jsonData:
            fields.append("{0}=\"{1}\"".format(key, jsonData.get(key)))

    # Extract path
    if "path" in jsonData:
        fields.append(parsePath(jsonData.get("path")))

    # Extract comment
    if "comment" in jsonData:
        comment = parseTextString(jsonData.get("comment"), "comment")
        if len(jsonData.get("comment")) > 0:
            fields.append(comment)

    # Extract telemetry
    fields = parseTelemetry(jsonData, fields)

    # Extract weather data
    fields = parseWeather(jsonData, fields)

    # Extract raw packet
    if "raw" in jsonData:
        comment = parseTextString(jsonData.get("raw"), "raw")
        if len(jsonData.get("raw")) > 0:
            fields.append(comment)

    # Extract APRS symbol
    if "symbol" in jsonData:
        comment = parseTextString(jsonData.get("symbol"), "symbol")
        if len(jsonData.get("symbol")) > 0:
            fields.append(comment)

    # Extract APRS symbol table
    if "symbol_table" in jsonData:
        comment = parseTextString(jsonData.get("symbol_table"), "symbol_table")
        if len(jsonData.get("symbol_table")) > 0:
            fields.append(comment)

    # Combine final valid line protocol string
    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseWX(jsonData):
    """Parse WX APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    ## Schema
    # measurement = packet*
    # field = from
    # field = to
    # tag = format
    # field = via
    # field = wx_raw_timestamp
    # field = comment
    # field = humidity
    # field = pressure
    # field = rain_1h
    # field = rain_24h
    # field = rain_since_midnight
    # field = temperature
    # field = wind_direction
    # field = wind_gust
    # field = wind_speed
    # field = path
    # field = raw

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    # Obtain tags
    tags.append("format={0}".format(jsonData.get("format")))

    # Join tags into comma separated string
    tagStr = ",".join(tags)

    # Create field key lists to iterate through
    fieldTextKeys = ["from", "to", "via"]

    # Extract text fields from packet
    for key in fieldTextKeys:
        if key in jsonData:
            fields.append("{0}=\"{1}\"".format(key, jsonData.get(key)))

    # Extract path
    if "path" in jsonData:
        fields.append(parsePath(jsonData.get("path")))

    # Extract comment
    if "comment" in jsonData:
        comment = parseTextString(jsonData.get("comment"), "comment")
        if len(jsonData.get("comment")) > 0:
            fields.append(comment)

    # Extract raw from packet
    if "raw" in jsonData:
        comment = parseTextString(jsonData.get("raw"), "raw")
        if len(jsonData.get("raw")) > 0:
            fields.append(comment)

    # Extract wx_raw_timestamp from packet
    if "wx_raw_timestamp" in jsonData:
        rawtimestamp = parseTextString(jsonData.get("wx_raw_timestamp"), "wx_raw_timestamp")
        if len(jsonData.get("wx_raw_timestamp")) > 0:
            fields.append(rawtimestamp)

    # Obtain weather data
    fields = parseWeather(jsonData, fields)

    # Combine final valid line protocol string
    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseBeacon(jsonData):
    """Parse Beacon APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    ## Schema
    # measurement = packet
    # field = from
    # field = to
    # tag = format
    # field = via
    # field = text
    # field = path
    # field = raw

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    # Obtain tags
    tags.append("format={0}".format(jsonData.get("format")))

    # Join tags into comma separated string
    tagStr = ",".join(tags)

    # Create field key lists to iterate through
    fieldTextKeys = ["from", "to", "via"]

    # Extract text fields from packet
    for key in fieldTextKeys:
        if key in jsonData:
            fields.append("{0}=\"{1}\"".format(key, jsonData.get(key)))

    # Extract path
    if "path" in jsonData:
        fields.append(parsePath(jsonData.get("path")))

    # Extract text
    if "text" in jsonData:
        comment = parseTextString(jsonData.get("text"), "text")
        if len(jsonData.get("text")) > 0:
            fields.append(comment)

    # Extract raw packet
    if "raw" in jsonData:
        comment = parseTextString(jsonData.get("raw"), "raw")
        if len(jsonData.get("raw")) > 0:
            fields.append(comment)

    # Combine final valid line protocol string
    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseBulletin(jsonData):
    """Parse Bulletin APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    ## Schema
    # measurement = packet
    # field = from
    # field = to
    # tag = format
    # field = via
    # field = message_text
    # field = bid
    # field = identifier
    # field = path
    # field = raw

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    # Obtain tags
    tags.append("format={0}".format(jsonData.get("format")))

    # Join tags into comma separated string
    tagStr = ",".join(tags)

    # Create field key lists to iterate through
    fieldNumKeys = ["bid"]
    fieldTextKeys = ["from", "to", "via"]

    # Extract number fields from packet
    for key in fieldNumKeys:
        if key in jsonData:
            fields.append("{0}={1}".format(key, jsonData.get(key)))

    # Extract text fields from packet
    for key in fieldTextKeys:
        if key in jsonData:
            fields.append("{0}=\"{1}\"".format(key, jsonData.get(key)))

    # Extract path
    if "path" in jsonData:
        fields.append(parsePath(jsonData.get("path")))

    # Extract message text
    if "message_text" in jsonData:
        message = parseTextString(jsonData.get("message_text"), "message_text")
        if len(jsonData.get("message_text")) > 0:
            fields.append(message)

    # Extract identifier
    if "identifier" in jsonData:
        identifier = parseTextString(jsonData.get("identifier"), "identifier")
        if len(jsonData.get("identifier")) > 0:
            fields.append(identifier)

    # Extract raw packet
    if "raw" in jsonData:
        comment = parseTextString(jsonData.get("raw"), "raw")
        if len(jsonData.get("raw")) > 0:
            fields.append(comment)

    # Combine final valid line protocol string
    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseMessage(jsonData):
    """Parse Message APRS packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """
    ## Schema
    # measurement = packet
    # field = from
    # field = to
    # tag = format
    # field = via
    # field = addresse
    # field = message_text
    # field = path
    # field = raw
    # field = msgNo
    # field = response

    # initialize variables
    tags = []
    fields = []

    # Set measurement to "packet"
    measurement = "packet"

    # Obtain tags
    tags.append("format={0}".format(jsonData.get("format")))

    # Join tags into comma separated string
    tagStr = ",".join(tags)

    # Create field key lists to iterate through
    fieldNumKeys = ["msgNo"]
    fieldTextKeys = ["from", "to", "via", "addresse"]

    # Extract number fields from packet
    for key in fieldNumKeys:
        if key in jsonData:
            fields.append("{0}={1}".format(key, jsonData.get(key)))

    # Extract text fields from packet
    for key in fieldTextKeys:
        if key in jsonData:
            fields.append("{0}=\"{1}\"".format(key, jsonData.get(key)))

    # Extract path
    if "path" in jsonData:
        fields.append(parsePath(jsonData.get("path")))

    # Extract message text
    if "message_text" in jsonData:
        message = parseTextString(jsonData.get("message_text"), "message_text")
        if len(jsonData.get("message_text")) > 0:
            fields.append(message)

    # Extract response
    if "response" in jsonData:
        message = parseTextString(jsonData.get("response"), "response")
        if len(jsonData.get("response")) > 0:
            fields.append(message)

    # Extract raw from packet
    if "raw" in jsonData:
        comment = parseTextString(jsonData.get("raw"), "raw")
        if len(jsonData.get("raw")) > 0:
            fields.append(comment)

    # Combine final valid line protocol string
    fieldsStr = ",".join(fields)

    return measurement + "," + tagStr + " " + fieldsStr


def parseTelemetryScaling(jsonData):
    """Parse Telemetry-Message APRS scaling value packets into influxedb line protocol

    keyword arguments:
    jsonData -- aprslib parsed JSON packet
    """

    # Parse packet for equations
    equations = parseEquations(jsonData)

    if equations:
        # If equations present, then add to dictionary of station
        # This is not ideal but required until Grafana supports SELECT queries
        # in templates.
        telemetryDictionary[jsonData.get("from")] = equations


def parseTextString(rawText, name):
    '''Parse text strings for invalid characters. Properly escape for
    line protocol strings if found.

    keyword arguments:
    rawText -- String to be checked
    name -- Name of field
    '''

    # Check if length is valid
    if len(rawText) > 0:
        try:
            # Convert to ASCII and replace invalid characters
            text = rawText.encode('ascii', 'replace')
            text = text.replace("\\", "\\\\")
            text = text.replace("\'", "\\\'")
            text = text.replace("\"", "\\\"")

            # Create valide libe protocol field string
            textStr = ("{0}=\"{1}\"".format(name, text))

        except UnicodeError as e:
            logger.error(e)

        except TypeError as e:
            logger.error(e)

        # Return text string if line protocol format
        return textStr

    else:
        # rawText is <= 0
        # Return text string if line protocol format
        return rawText


def parsePath(path):
    """Take path and turn into a string

    keyword arguments:
    path -- list of paths from aprslib
    """

    # Join path items into a string separated by commas, valid line protocol
    temp = ",".join(path)
    pathStr = ("path=\"{0}\"".format(temp))

    # Return line protocol string
    return pathStr


def callback(packet):
    """aprslib callback for every packet received from APRS-IS connection

    keyword arguments:
    packet -- APRS-IS packet from aprslib connection
    """
    # Open a new connection to influxdb, parse the packet into line protocol
    influxConn = connectInfluxDB()
    line = jsonToLineProtocol(packet)

    # Check for line protocol string
    if line:
        # Write string to database
        try:
            influxConn.write_points([line], protocol='line')

        except StandardError:
            # An error occured before writing to influxdb
            logger.error('A StandardError occured', exc_info=True)

        except influxdb.exceptions.InfluxDBClientError:
            # An error occured in the request
            logger.error('An error occured in the request', exc_info=True)
            logger.error("Line Protocol: {0}".format(line))

        except influxdb.exceptions.InfluxDBServerError:
            # An error occured in the server
            logger.error('An error occured in the server', exc_info=True)
            logger.error("Line Protocol: {0}".format(line))


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
    interval -- Minutes between status messages
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

    # Create telemetry dictionary
    global telemetryDictionary
    telemetryDictionary = {}

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

    # Set aprslib logger equal to aprs2influxdb logger
    AIS.logger = logger

    # Connect to APRS-IS servers
    try:
        AIS.connect()

    except aprslib.exceptions.LoginError:
        # An error occured
        logger.error('An aprslib LoginError occured', exc_info=True)

    except aprslib.exceptions.ConnectionError:
        # An error occured
        logger.error('An aprslib ConnectionError occured', exc_info=True)

    # Create heartbeat
    t1 = threading.Thread(target=heartbeat, args=(AIS, args.callsign, args.interval))

    # Create consumer
    t2 = threading.Thread(target=consumer, args=(AIS,))

    # Start threads
    t1.start()
    t2.start()


if __name__ == "__main__":
    main()
