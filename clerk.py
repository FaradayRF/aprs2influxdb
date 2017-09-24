import aprslib
import configparser
from influxdb import InfluxDBClient

def jsonToLineProtocol(jsonData):
    #Converts aprslib JSON to influxdb line protocol

    line = "packets,from={0} latitude={1},longitude={2},altitude={3},analog0={4},analog1={5},analog2={6},analog3={7},analog4={8}"
    try:
        a = line.format(jsonData["from"],jsonData["latitude"],jsonData["longitude"],jsonData["altitude"],jsonData["telemetry"]["vals"][0],jsonData["telemetry"]["vals"][1],jsonData["telemetry"]["vals"][2],jsonData["telemetry"]["vals"][3],jsonData["telemetry"]["vals"][4])
        print a
	return a
    except StandardError as e:
        pass

def callback(packet):
    packet = aprslib.parse(packet)
#    if packet['to'] == 'GPSFDY':
    #print packet
    #slowwwwww
    influxConn = connectInfluxDB()
    a = jsonToLineProtocol(packet)
    if a:
        print a
        influxConn.write_points([a],protocol='line')

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
