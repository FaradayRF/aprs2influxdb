import aprslib
import configparser
import influxdb
from influxdb import InfluxDBClient
import Geohash

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

        measurement = "packet"

        try:
            tags.append("from={0}".format(jsonData["from"]))
            tags.append("to={0}".format(jsonData["to"]))
            tags.append("symbolTable={0}".format(jsonData["symbol_table"]))
            tags.append("symbol={0}".format(jsonData["symbol"]))
            tags.append("format={0}".format(jsonData["format"]))
            tags.append("comment={0}".format(jsonData["comment"]))
            tagStr = ",".join(tags)

        except KeyError as e:
            print e
            print jsonData

        try:
            fields.append("latitude={0}".format(jsonData["latitude"]))
            fields.append("longitude={0}".format(jsonData["longitude"]))
            fields.append("posAmbiguity={0}".format(jsonData["posambiguity"]))
            fields.append("altitude={0}".format(jsonData["altitude"]))
            fields.append("speed={0}".format(jsonData["speed"]))
        except KeyError as e:
            print e
            print jsonData

        if jsonData["telemetry"]["seq"]:
            try:
                fields.append("sequenceNumber={0}".format(jsonData["telemetry"]["seq"]))
                fields.append("analog1={0}".format(jsonData["telemetry"]["vals"][0]))
                fields.append("analog2={0}".format(jsonData["telemetry"]["vals"][1]))
                fields.append("analog3={0}".format(jsonData["telemetry"]["vals"][2]))
                fields.append("analog4={0}".format(jsonData["telemetry"]["vals"][3]))
                fields.append("analog5={0}".format(jsonData["telemetry"]["vals"][4]))
                fields.append("digital={0}".format(jsonData["telemetry"]["bits"]))

            except KeyError as e:
                print e
                print jsonData

        fieldsStr = ",".join(fields)

        lineProtocolStr = " ".join(measurement,tagStr,fieldsStr)
        print lineProtocolStr

    #line = "packet,from={0},geohash={9} latitude={1},longitude={2},altitude={3},analog0={4},analog1={5},analog2={6},analog3={7},analog4={8}"

    # try:
    #     a = line.format(jsonData["from"],jsonData["latitude"],jsonData["longitude"],jsonData["altitude"],jsonData["telemetry"]["vals"][0],jsonData["telemetry"]["vals"][1],jsonData["telemetry"]["vals"][2],jsonData["telemetry"]["vals"][3],jsonData["telemetry"]["vals"][4],str(geohashVal))
    #     print a
	# return a
    # except StandardError as e:
    #     pass

    #except InfluxDB.ClientError as e:
    #    print e

def callback(packet):
    packet = aprslib.parse(packet)
#    if packet['to'] == 'GPSFDY':
    #print packet
    #slowwwwww
    influxConn = connectInfluxDB()
    a = jsonToLineProtocol(packet)
    if a:
        print a
	try:
            influxConn.write_points([a],protocol='line')

        except StandardError as e:
            pass

        except influxdb.exceptions.InfluxDBClientError as e:
            print e


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
    # Open APRS-IS connection
    AIS = aprslib.IS("KB1LQC")
    AIS.connect()

    # Obtain raw APRS-IS packets and sent to callback when received
    AIS.consumer(callback, raw=True)

if __name__ == "__main__":
    main()
