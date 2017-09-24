import aprslib

def callback(packet):
    packet = aprslib.parse(packet)
    if packet['to'] == 'GPSFDY':
        print packet

def main():
    # Open APRS-IS connection
    AIS = aprslib.IS("KB1LQC")
    AIS.connect()
    # Obtain raw APRS-IS packets and sent to callback when received
    AIS.consumer(callback, raw=True)

if __name__ == "__main__":
    main()
