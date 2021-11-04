#!/usr/bin/python3
#
# arima_geo.py - ARMA forecast model with geo-positional data
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
        'geo_index': args.geo_index,
        'geo_zoomlevel_auto': [8, 14, 20] if args.geo_multiscale else [],
        'geo_zoomlevel': args.geo_zoomlevel,
        'autoclean_off': True,
        'max_sensor': max_sensor,
        'geo_position': stationary
    })


def read(args, creds):
    r = create_clients(args.test, creds)
    if r is None:
        raise ValueError("unknown test scenario: %d" % args.test)

    (user, swimlane, attrs) = r
    header, body, max_sensor, labels, stationary, data = arima.import_csv(args, CSV)

    ms0 = time.time()
    if args.query == 1:
        if args.geo_index == 'all':
            query = """
                data = select sum($w) geo box [34.39, -24.4383728504, 71.28, 40.34] group $all by time as w end,
                model = arima_model(data; p: %d, q: %d, d: 1),
                arima_forecast(data; params: model, n: %d, alpha: 0.05).
            """ % (args.model_p, args.model_q, args.model_n)
        elif args.geo_index == 'swimlane':
            query = """
                data = select sum($w) geo box [34.39, -24.4383728504, 71.28, 40.34] group $all by time as w end,
                model = arima_model(data; p: %d, q: %d, d: 1),
                arima_forecast(data; params: model, n: %d, alpha: 0.05).
            """ % (args.model_p, args.model_q, args.model_n)
        else:
            query = """
                data = select sum($w) geo box [34.39, -24.4383728504, 71.28, 40.34] group $0-$%d by time as w end,
                model = arima_model(data; p: %d, q: %d, d: 1),
                arima_forecast(data; params: model, n: %d, alpha: 0.05).
            """ % (max_sensor - 1, args.model_p, args.model_q, args.model_n)
        print(query)
        (ok, r) = swimlane.query(query)
        assert ok == 'ok', (ok, r)
        print(json.dumps(r, indent=4))
    elif args.query == 2:
        bbox = [
            [31.2048, 30.0206, 31.2466, 30.0539],
            [31.1279, 29.9183, 31.4361, 30.1905],
            [31.8279, 29.9183, 32.3533, 31.0984],
            [31.1279, 29.9183, 32.8533, 31.3984],
            [27.2827, 26.5704, 32.8533, 31.3984],
            [24.9316, 22.0465, 35.7097, 31.3984],
            [25, 20, 37, 33],
            [24.9316, 11.6324, 46.3445, 31.3984],
            [23.5253, 3.4493, 60.5828, 37.8295],
            [-16.9044, 3.4493, 60.5828, 60.9985],
            [-16.2013, -37.7694, 95.3875, 58.8863],
            [-21.8263, -39.1458, 138.2781, 74.8126],
            [-79.8341, -39.1458, 138.2781, 84.2812],
            [-166.67, -69.31, 138.2781, 84.2812]
        ]
        for p in bbox:
            read_tiles(args, swimlane, p)

    print("OK: Data are read, scenario: %d, elapsed: %ss" % (args.query, round((time.time() - ms0) * 1000) / 1000.0))


def read_tiles(args, swimlane, bbox):
    [lat1, lng1, lat2, lng2] = bbox
    q = "select sum($w) geo box [%s, %s, %s, %s] group $all by time as w format json (array: true) end." % (
        lng1, lat1, lng2, lat2
    )
    print(q)
    resp = swimlane.query(q)
    (ok, r) = resp
    assert ok == 'ok' and 'data' in r and 'values' in r['data'][0], (ok, r)
    dataset = r['data'][0]['values']

    if args.verbose:
        r['data'][0]['values'] = {}
        print("Server read details:")
        print(json.dumps(r, indent=4))
    try:
        print("server read time: %d ms" % r['data'][0]['ms'])
    except KeyError:
        pass

    return True

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
    parser.add_argument("-t", "--test", type=int, choices=range(1, 21), help="test scenario number", default=1)
    parser.add_argument('-w','--write', help='Write data', required=False, action='store_true')
    parser.add_argument('-v','--validate', help='Validate data', required=False, action='store_true')
    parser.add_argument('-d','--delete', help='Clean data', required=False, action='store_true')
    parser.add_argument('-i','--info', help='Print info about test scenario/swimlane', required=False, action='store_true')
    parser.add_argument("-q", "--query", type=int, choices=range(1, 11), help="Read scenario: 1 - read, 2 - model", required=False, default=1)
    parser.add_argument('--read_back', help='Validate write by read back', required=False, action='store_true', default=False)
    parser.add_argument('--verbose', help='verbose: True or False', required=False, action='store_true', default=False)
    parser.add_argument('--creds', help="file with credential info", required=False)
    parser.add_argument('--csv', help="CSV file with timeseries to import", required=False)
    parser.add_argument('--model_p', type=int, choices=range(1, 8), help="AR model order", required=False, default=5)
    parser.add_argument('--model_q', type=int, choices=range(1, 6), help="MA model order", required=False, default=3)
    parser.add_argument('--model_n', type=int, help="forecast number", required=False, default=20)
    parser.add_argument('--geo_index', help='Value of "geo_index"', type=str, choices=['all', 'sensor', 'swimlane'], required=False, default='all')
    parser.add_argument('--geo_zoomlevel', help='Value of "geo_zoomlevel"', type=int, choices=range(1, 21), required=False, default=20)
    parser.add_argument('--geo_multiscale', help='Apply "geo_zoomlevel_auto": "multiscale"', required=False, action='store_true', default=False)

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
