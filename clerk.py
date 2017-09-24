import aprslib
import configparser
from influxdb import InfluxDBClient

def callback(packet):
    packet = aprslib.parse(packet)
    if packet['to'] == 'GPSFDY':
        print packet
        #slowwwwww
        influxConn = connectInfluxDB()
        json_body = [
            {
                "measurement": "packets",
                "tags": {
                    "callsign": packet['from']
                },
                "fields": {
                    "latitude": packet['latitude'],
                    "longitude": packet['longitude']
                }
            }
        ]
        influxConn.write(json_body)

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
