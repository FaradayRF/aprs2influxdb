import aprslib
import configparser
from influxdb import InfluxDBClient

def callback(packet):
    packet = aprslib.parse(packet)
    if packet['to'] == 'GPSFDY':
        print packet

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

    client = InfluxDBClient(host, port, user, password, dbname)
    print client

    query = 'select * from cpu;'
    result = client.query(query)
    print result

def main():
    # Open APRS-IS connection
    AIS = aprslib.IS("KB1LQC")
    AIS.connect()
    connectInfluxDB()
    # Obtain raw APRS-IS packets and sent to callback when received
    AIS.consumer(callback, raw=True)
    print "woot"

if __name__ == "__main__":
    main()
