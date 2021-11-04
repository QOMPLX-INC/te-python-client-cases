#!/usr/bin/python3
#
# arima_misc_geo.py - ARMA forecast model with geo-positional data
#

import argparse, os, sys, csv, json, time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../common')))
import arima, utils
from utils import ConnectionError, create_clients, open_creds, print_info, clean

CREDS = 'arima_geo.json'
CSV = 'covid_time_series.csv'

#############################################################################

def write(args, creds):
    arima.write1(args, creds, lambda max_sensor, stationary: {
        'time_slice': 8640000,
        #'geo_index': 'sensor',
        #'geo_index': 'swimlane',
        'geo_index': 'all',
        'geo_zoomlevel': 5,
        'autoclean_off': True,
        'max_sensor': max_sensor,
        'geo_position': stationary
    })


def read(args, creds):
    r = create_clients(args.test, creds)
    if r is None:
        raise ValueError("unknown test scenario: %d" % args.test)

    country = args.model_country # "Germany"
    country_idx = [k for k, v in creds['info']['labels'].items() if v == country]
    country_idx = int(country_idx[0])
    print(country, "index:", country_idx)

    (user, swimlane, attrs) = r

    if args.query == 1:
        query = """
                data = select $all geo box [40, 0, 60, 30] end,
                model = arima_model(data; p: %d, q: %d, d: 1),
                arima_forecast(data; params: model, n: %d, alpha: 0.05).
            """ % (args.model_p, args.model_q, args.model_n)
        print(query)
        (ok, r) = swimlane.query(query)
        assert ok == 'ok', (ok, r)
        print(r)
    elif args.query == 2:
        query = "select count($all) geo box [40, 0, 60, 30] end."
        print(query)
        (ok, r) = swimlane.query(query)
        assert ok == 'ok', (ok, r)
        print(json.dumps(r, indent=4))
    elif args.query == 3:
        query = """
            select sum($w) geo box [40, 0, 60, 30] group $all by time as w format json (array: true) end.
        """
        print(query)
        (ok, r) = swimlane.query(query)
        assert ok == 'ok', (ok, r)
        print(json.dumps(r, indent=4))
    elif args.query == 6:
        query = "select count($%d) end." % country_idx
        print(query)
        (ok, r) = swimlane.query(query)
        assert ok == 'ok', (ok, r)
        print(json.dumps(r, indent=4))
    elif args.query == 7:
        query = "select (incremental_select: false) $0 format text end."
        print(query)
        (ok, r) = swimlane.query(query)
        assert ok == 'ok', (ok, r)
        print(json.dumps(r, indent=4))
    elif args.query == 8:
        query = "select $0 format text end."
        print(query)
        (ok, r) = swimlane.query(query)
        assert ok == 'ok', (ok, r)
        print(json.dumps(r, indent=4))
    elif args.query == 9:
        query = "select (incremental_select: false) $0 format json (array: true) end."
        print(query)
        (ok, r) = swimlane.query(query)
        assert ok == 'ok', (ok, r)
        print(json.dumps(r, indent=4))
    elif args.query == 10:
        query = "select $0 format json (array: true) end."
        print(query)
        (ok, r) = swimlane.query(query)
        assert ok == 'ok', (ok, r)
        print(json.dumps(r, indent=4))

    print("OK: Data are read, scenario: %d" % args.query)

#############################################################################

def main(args):
    r = None
    try:
        creds = open_creds(args, CREDS)
        if args.write:
            r = write(args, creds)
        elif args.validate:
            r = arima.validate(args, creds)
        elif args.info:
            r = print_info(args, creds)
        else:
            key = str(args.test)
            if key in creds:
                if args.delete:
                    r = clean(args, creds)
                else:
                    r = read(args, creds)
            else:
                print("test scenario is not supported: %d" % args.test)
    except ConnectionError as e:
        print(e)
    return r

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TimeEngine Python Client Test')
    parser.add_argument('-s','--server', help='TimeEngine server host', required=False)
    parser.add_argument('-p','--port', help='TimeEngine server port', required=False)
    parser.add_argument('--use_https', help='Use https scheme', required=False, default=False, action='store_true')
    parser.add_argument('--user', help='TimeEngine User key', required=False)
    parser.add_argument('--secret', help='TimeEngine User secret', required=False)
    parser.add_argument("-t", "--test", type=int, choices=range(1, 11), help="test scenario number", default=1)
    parser.add_argument('-w','--write', help='Write data', required=False, action='store_true')
    parser.add_argument('-v','--validate', help='Validate data', required=False, action='store_true')
    parser.add_argument('-d','--delete', help='Clean data', required=False, action='store_true')
    parser.add_argument('-i','--info', help='Print info about test scenario/swimlane', required=False, action='store_true')
    parser.add_argument("-q", "--query", type=int, choices=range(1, 11), help="Read scenario: 1 - read, 2 - model", required=False, default=1)
    parser.add_argument('--read_back', help='Validate write by read back', required=False, action='store_true', default=False)
    parser.add_argument('--verbose', help='verbose: True or False', required=False, action='store_true', default=False)
    parser.add_argument('--creds', help="file with credential info", required=False)
    parser.add_argument('--csv', help="CSV file with timeseries to import", required=False)
    parser.add_argument("-mc", "--model_country", help="country for query", required=False, default="Germany")
    parser.add_argument("-mp", "--model_p", type=int, choices=range(1, 8), help="AR model order", required=False, default=5)
    parser.add_argument("-mq", "--model_q", type=int, choices=range(1, 6), help="MA model order", required=False, default=3)
    parser.add_argument("-mn", "--model_n", type=int, help="forecast number", required=False, default=20)

    args = parser.parse_args()

    if args.server != None:
        utils.HOST = args.server
    if args.port != None:
        utils.PORT = int(args.port)
    if args.use_https != None:
        utils.ISHTTPS = args.use_https
    if args.user != None:
        utils.MasterKey = args.user
    if args.secret != None:
        utils.MasterSecret = args.secret

    try:
        main(args)
    except ConnectionError as e:
        print(e)

#############################################################################
