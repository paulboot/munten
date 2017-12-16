#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# COPYRIGHT

# Dependancies
# sudo apt-get update
# sudo apt-get install build-essential
# sudo apt-get install python3-dev
# sudo pip3 install scrapy


from secret import *
import scrapy
import requests
from bs4 import BeautifulSoup
import time
import sys
import ssl
import argparse
import datetime
import logging
import json
from influxdb import InfluxDBClient





class AllianzSpider(scrapy.Spider):
    name = "allianz_spider"
    start_urls = [URL1]

    def parse(self, response):
        FONDS_SELECTOR = '.az-mdl-rateChange'
        for fonds in response.css(FONDS_SELECTOR):
            NAME_SELECTOR = 'a::text'
            yield {
                'name': fonds.css('a::text').extract_first(),
                'rate': fonds.css('.az-cmp-rateChange::text').extract_first(),
            }




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

    page = requests.get(URL1)
    soup = BeautifulSoup(page.text, 'html.parser')

    payload = "FondsAllianz,currency=€ "
    fonds_list = soup.find_all(class_='az-mdl-rateChange')
    for fonds in fonds_list:
        nameStr = fonds.find('a').string.replace("Allianz ", "")
        (currencyStr, valueStr) = fonds.find(class_='az-cmp-rateChange').string.split()
        payload += nameStr.replace(" ", "_") + "=" + valueStr + ","
        print(timenownano(), nameStr, currencyStr, valueStr)

    payload = payload[:-1] + " " + str(timenownano())
    print(payload)

    sys.exit()

    influxDbClient = setupdb(influxDbHost, influxDbPort, influxDbUser, influxDbPassword, influxDbName)




    getdata()

    insertdata()

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
