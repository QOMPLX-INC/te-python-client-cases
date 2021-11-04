#!/usr/bin/python3
#
# arima.py - simple ARMA forecast model
#

import argparse, os, sys, csv, json, time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../common')))
import utils
from utils import (new_user, new_swimlane,
                   ConnectionError,
                   create_clients, update_clients, open_creds,
                   clean, print_info,
                   import_csv, country_label, HOST, PORT, ISHTTPS)

CREDS = 'arima.json'
CSV = 'covid_time_series.csv'

#############################################################################

def write(args, creds):
    write1(args, creds, lambda max_sensor, stationary: {
        'time_slice': 8640000,
        'autoclean_off': True,
        'max_sensor': max_sensor
    })

def write1(args, creds, sw_opts_fun):
    r = create_clients(args.test, creds)
    if r is not None:
        print("test scenario %d already exists" % args.test)
        return

    print("create test scenario: %d" % args.test)
    header, body, max_sensor, labels, stationary, data = import_csv(args, CSV)

    sw_opts = sw_opts_fun(max_sensor, stationary)
    user = new_user()
    swimlane = new_swimlane(user, sw_opts, {})

    env_labels = ','.join(['%d: "%s"' % (int(no), nm) for (no, nm) in labels.items()])
    print(env_labels)
    (ok, r) = swimlane.query("""env labels: #{%s} end.""" % env_labels)
    assert ok == 'ok' and 'status' in r and r['status'] == 1, (ok, r)

    if args.verbose:
        (ok, r) = user.query('get_swimlane_opts("%s").' % swimlane.app_key)
        print(json.dumps(r, indent=4))

    (ok, r) = user.insert([{
        'key': swimlane.app_key,
        'data': data
    }])
    assert ok == 'ok', (ok, r)

    if args.verbose:
        print("Server write details:")
        print(json.dumps(r, indent=4))
    try:
        print("server write time: %d ms" % r['batch'][swimlane.app_key]['result']['info']['ms'])
    except KeyError:
        pass

    if args.read_back:
        validate1(args, user, swimlane)

    update_clients(args.test, creds, user, swimlane, {'max_sensor': max_sensor, 'labels': labels})


def read(args, creds):
    r = create_clients(args.test, creds)
    if r is None:
        raise ValueError("unknown test scenario: %d" % args.test)

    country = args.model_country # "Germany"
    country_idx = [k for k, v in creds['info']['labels'].items() if v == country]
    country_idx = int(country_idx[0])
    print(country, "index:", country_idx)

    (user, swimlane, attrs) = r

    query = """
        data = select $%d end,
        model = arima_model(data; p: %d, q: %d, d: 1),
        arima_forecast(data; params: model, n: %d, alpha: 0.05).
    """ % (country_idx, args.model_p, args.model_q, args.model_n)
    print(query)

    (ok, r) = swimlane.query(query)
    assert ok == 'ok', (ok, r)
    print(json.dumps(r, indent=4))


def validate(args, creds):
    r = create_clients(args.test, creds)
    if r is None:
        raise ValueError("unknown test scenario: %d" % args.test)

    (user, swimlane, attrs) = r
    validate1(args, user, swimlane)
    print("OK: Data are validated")


def validate1(args, user, swimlane):
    header, body, max_sensor, labels, stationary, data = import_csv(args, CSV)
    resp = swimlane.query("select $0-$%d format json (array: true) end." % (max_sensor - 1))
    (ok, r) = resp
    assert ok == 'ok' and 'data' in r and 'values' in r['data'][0], (ok, r)
    dataset = r['data'][0]['values']

    for sensor, values in dataset.items():
        no = int(sensor)
        d = body[no]
        label = country_label(d)
        assert label == labels[sensor]
        i = 0
        for record in values:
            assert data[i][sensor] == record["value"], (data[i][sensor], record["value"])
            i += 1

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
            r = validate(args, creds)
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
    parser.add_argument('--read_back', help='Validate write by read back', required=False, action='store_true', default=False)
    parser.add_argument('--verbose', help='verbose: True or False', required=False, action='store_true', default=False)
    parser.add_argument('--creds', help="file with credential info", required=False)
    parser.add_argument('--csv', help="CSV file with timeseries to import", required=False)
    parser.add_argument('--model_country', help="country for query", required=False, default="Germany")
    parser.add_argument('--model_p', type=int, choices=range(1, 8), help="AR model order", required=False, default=5)
    parser.add_argument('--model_q', type=int, choices=range(1, 6), help="MA model order", required=False, default=3)
    parser.add_argument('--model_n', type=int, help="forecast number", required=False, default=20)

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
