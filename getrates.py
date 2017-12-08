#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# COPYRIGHT


from secret import *
import time
import sys
import ssl
import argparse
import datetime
import logging
import json
from influxdb import InfluxDBClient



def timenownano():
    return "%18.f" % (time.time() * 10 ** 9)


def setupdb(influxDbHost, influxDbPort, influxDbUser, influxDbPassword, influxDbName):

    print("Connect to DB: %s %i" % (influxDbHost, influxDbPort))
    client = InfluxDBClient(influxDbHost, influxDbPort, influxDbUser, influxDbPassword, influxDbName)

    print("Create database: " + influxDbName)
    client.create_database(influxDbName)

    print("Create a retention policy")
    client.create_retention_policy('365d_policy', '365d', 365, default=True)

    print("Switch user: " + influxDbName)
    client.switch_user(influxDbName, influxDbPassword)

    return client


def insertindb(client, line):

    print("Write points: {0}".format(line))
    client.write_points(line, time_precision='ms', protocol='line')

    return True


def callback(ch, method, properties, body):
    print(" [x] rk: {}, headers: {}, msg: {}, ts: {}".
          format(method.routing_key, properties.headers, body,
                 datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")))

    dict = json.loads(str(body, 'utf-8'))
    if 'assetpi' in dict:
        if 'data' in dict['assetpi']:
            data = dict['assetpi']['data']
            payload = "WaterTemperature,gateway=gw-zh-delft01,units=" + data['units'] + " " +\
                      "average=" + str(data['temperature']) + " " +\
                      str(data['timestamp'])
        elif 'errors' in dict['assetpi']:
            print("Error: invalid data %s" % dict['assetpi']['errors'])
            #ToDo Store Error in database using different format
        else:
            print("Error: unknown key found in dict")
    else:
        print("Error: no assetpi key found, ignoring message")




def main():
    logging.basicConfig(level=logging.INFO)
    
    parser = argparse.ArgumentParser(description="AMQP client")
    parser.add_argument('--version', action='version', version='1.0')
    args = parser.parse_args()
    
    queue_name = user + ':' + datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    credentials = pika.PlainCredentials(user, password)

    influxDbClient = setupdb(influxDbHost, influxDbPort, influxDbUser, influxDbPassword, influxDbName)

    getgata()

    insertgata()

    try:
        # Setup our ssl options
        getData()
        insertindb(influxDbClient, getData())


    except IndexError:
        logging.error("label format error, should be < x=y >!")
        sys.exit()
    except Exception as e:
        logging.error("Error!")
        logging.error(" %s", e.message)
        logging.error(type(e))
        sys.exit()

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
if __name__ == "__main__":
   main()
