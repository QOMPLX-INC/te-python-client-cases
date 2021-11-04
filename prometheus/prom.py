#!/usr/bin/python3
#
# prom.py - create a user for Prometheus remote write
#
#

from __future__ import print_function
import argparse, os, sys, json, random, time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../common')))

from mdtsdb import Mdtsdb
import utils
from utils import (new_user, ConnectionError, create_clients, update_clients, open_creds, HOST, PORT, REQ_TIMEOUT, ISHTTPS)

CREDS = 'prom.json'

MEASUREMENT = "default"
USER_ENV = """
user env (
    retention_policy: #{
        "expire": 86400*30
    },
    measurements: #{
        "%s": #{
            "opts": #{
                "time_slice": 900,
                "dense": true,
                "rconf_short_merge": true,
                "gather_sensors_stats": #{
                    "insert": false, "delete": false, "timeframe": false, "schema": false
                }
            },
            "series": ["__name__"]
            %%"series": ["__name__", "instance"]
        }
    }
) end,
get_report("measurements").
""" % (MEASUREMENT)


#############################################################################

def create(args, creds):
    r = create_clients(args.test, creds)
    if r is not None:
        print("test scenario %d already exists" % args.test)
        return

    print("create test scenario: %d" % args.test)
    su = get_su(args)
    (Ok, r) = su.get_or_create_adminkey(args.user, args.user_details)
    assert Ok == 'ok' and args.user == str(r['key']), "status: %s, message: %s, db user: %s, attempted super user: %s / %s" % (
        Ok, r, args.user, args.su_key, args.su_secret)
    q = """grant (prom: true) to user "%s" end.""" % args.user
    if args.verbose:
        print(q)
    (Ok, rq) = su.query(q)
    assert Ok == 'ok', (Ok, rq)

    user = Mdtsdb(
        host=utils.HOST,
        port=utils.PORT,
        admin_key=str(r['key']),
        secret_key=str(r['secret_key']),
        timeout=REQ_TIMEOUT,
        is_https=utils.ISHTTPS)
    if args.verbose:
        print(USER_ENV)
    (Ok, r) = user.query(USER_ENV)
    assert Ok == 'ok', (Ok, r)

    if args.verbose:
        print(json.dumps(r, indent=4, sort_keys=True))

    update_clients(args.test, creds, user, None, {})


def write(args, creds):
    return write1(args, creds, args.num)


def write1(args, creds, n):
    r = create_clients(args.test, creds)
    if r is None:
        raise ValueError("unknown test scenario: %d" % args.test)

    (user, _, attrs) = r
    (ok, sws) = user.query("get_swimlanes().")
    assert ok == 'ok'

    t0 = int(time.time())
    for job in ['node', 'prometheus']:
        series = {
            'job': job,
            'instance': 'localhost:9100'
        }
        data = []
        for ti in range(t0, t0 + n):
            data.append(datafun(args, ti))
        payload = [{
            'measurement': MEASUREMENT,
            'series': series,
            'data': data
        }]
        if args.verbose:
            print(payload)
        (Ok, r) = user.insert(payload)

    if args.verbose:
        print("Server write details:")
        print(json.dumps(r, indent=4, sort_keys=True))
    try:
        for _, v in r['batch'].items():
            print("server write time: %d ms" % v['result']['info']['ms'])
    except KeyError:
        pass

    print("OK: Data are written, scenario: %d" % args.test)


def update_env(args, creds):
    r = create_clients(args.test, creds)
    if r is None:
        raise ValueError("unknown test scenario: %d" % args.test)

    (user, _, attrs) = r

    if args.verbose:
        print(USER_ENV)
    (Ok, r) = user.query(USER_ENV)
    assert Ok == 'ok', (Ok, r)

    if args.verbose:
        print(json.dumps(r, indent=4, sort_keys=True))

    print("OK: User environment is updated: %d" % args.test)


def datafun_2(args, ti):
    if args.filter > 0 and ti % 3 == 0:
        d = {
            'ns': ti,
            'value': random.randint(0, 500),
            'series': {
                '__name__': 'nginx_server_uri_request_duration_seconds_bucket',
                'host': random.choice(['rabbitmq1.local', 'rabbitmq2.local', 'rabbitmq3.local']),
                'uri': random.choice(["q" + str(i) for i in range(10)]) # a label with an unbounded set of values
                #'rabbitmq_q_name': ''.join([chr(random.randint(97, 122)) for _ in range(16)]) # a label with an unbounded set of values
            }
        }
    elif args.filter > 1 and ti % 3 == 1:
        d = {
            'ns': ti,
            'value': random.randint(0, 500),
            'series': {
                '__name__': 'rabbitmq_q_%s' % ''.join([chr(random.randint(65, 90)) for _ in range(4)]), # a label with an unbounded set of values
                'host': random.choice(['rabbitmq1.local', 'rabbitmq2.local', 'rabbitmq3.local'])
            }
        }
    else:
        d = {
            'ns': ti,
            'value': random.randint(1000, 2000) / 10.0,
            'series': {
                '__name__': random.choice([
                    'node_network_iface_link',
                    'node_memory_Active_anon_bytes',
                    'node_memory_DirectMap2M_bytes'
                ])
            }
        }
        if d['series']['__name__'] == 'node_network_iface_link':
            d['series']['device'] = 'lo'
    return d


CHARS = [chr(i) for i in range(97, 123)]
HOSTS = [a + b for a in CHARS for b in CHARS]
def datafun(args, ti):
    d = {
        'ns': ti,
        'value': random.randint(1000, 2000) / 10.0,
        'series': {
            '__name__': random.choice([
                'node_network_iface_link',
                'node_memory_Active_anon_bytes',
                'node_memory_DirectMap2M_bytes'
            ]),
            'host': random.choice(HOSTS)
        }
    }
    if d['series']['__name__'] == 'node_network_iface_link':
        d['series']['device'] = 'lo'
    return d



def clean(args, creds):
    key = str(args.test)
    if key in creds:
        (user, _, _) = create_clients(args.test, creds)
        print("clean %s " % creds[key])

        (ok, sws) = user.query("get_swimlanes().")
        assert ok == 'ok', (ok, sws)
        for sw in sws:
            print(sw)
            (Ok, r) = user.delete_appkey(sw)
            assert Ok == 'ok' and 'status' in r and r['status'] == 1, (Ok, r)

        su = get_su(args)
        (Ok, r) = su.delete_adminkey(user.admin_key)
        assert Ok == 'ok' and 'status' in r and r['status'] == 1, (Ok, r)

        with open(creds['__path'], 'w') as fd:
            creds.pop(key, None)
            print(json.dumps(creds, indent=4), file=fd)
    else:
        print("unknown test scenario: %d" % args.test)

    return True


def print_info(args, creds):
    r = create_clients(args.test, creds)
    if r is not None:
        (user, _, attrs) = r
        print(json.dumps(attrs, indent=4))

        (ok, sws) = user.query("get_swimlanes().")
        assert ok == 'ok', (ok, sws)
        print("Created swimlanes:")
        total_sw = 0
        #for sw in sws:
        #    (Ok, resp) = user.query('get_swimlane_opts("%s").' % sw)
        #    if len(resp["labels"]) > 2 and len(resp["labels"]) < 9:
        #        print("%s (%s): %s" % (resp["key"], resp["opts"]["partition_info"]["__name__"], json.dumps(resp["labels"], indent=4)))
        for sw in sws:
            print("*" * 10)
            print(sw)
            (ok, r1) = user.query('get_swimlane_opts("%s").' % sw)
            (ok, r2) = user.query('get_report("describe_swimlane", #{"key" => "%s"}).' % sw)
            if args.verbose:
                print("Swimlane options:")
                print(json.dumps(r1, indent=4, sort_keys=True))
                print("Swimlane schema:")
                print(json.dumps(r2, indent=4, sort_keys=True))
            else:
                utilized_sensors = max(len(r1["labels"]), r2["utilized_sensors"])
                total = 0
                for _, v in r2["sensors"].items():
                    total += v["total_records"]
                total_sw += total
                print("    sensors: %d, records: %d, swimlane partition: %s" % (
                    utilized_sensors, total, r1["opts"]["partition_info"]))
                for k, v in r1["opts"].items():
                    if k != "partition_info" and k.startswith("part"):
                        print(k, v)
        print("total: swimlanes: %d, records: %d" % (len(sws), total_sw))
    else:
        print("unknown test scenario: %d" % args.test)


def print_size(args, creds):
    r = create_clients(args.test, creds)
    if r is not None:
        (user, _, attrs) = r
        print(json.dumps(attrs, indent=4))

        (ok, sws) = user.query("get_swimlanes().")
        assert ok == 'ok', (ok, sws)
        print("Created swimlanes:")
        cb = 0
        for sw in sws:
            cb += 1
            if cb % 1000 == 0:
                print("--- %d ---" % cb)
            (ok, r1) = user.query('get_swimlane_opts("%s").' % sw)
            utilized_sensors = len(r1["labels"])
            if utilized_sensors >= args.size:
                print("    sensors: %d, swimlane partition: %s" % (
                    utilized_sensors, r1["opts"]["partition_info"]))
                for k, v in r1["opts"].items():
                    if k != "partition_info" and k.startswith("part"):
                        print(k, v)
        print("total: swimlanes: %d" % len(sws))
    else:
        print("unknown test scenario: %d" % args.test)


def read_scenario(args, creds):
    r = create_clients(args.test, creds)
    if r is not None:
        (user, _, attrs) = r
        (ok, sws) = user.query("get_swimlanes().")
        if args.query == 1:
            d = []
            for sw in sws:
                (ok, r) = user.query('get_report("describe_swimlane", #{"key" => "%s"}).' % sw)
                assert ok == "ok", r
                d.append((r["utilized_sensors"], sw))
            (sensors, sw) = max(d)
            q = """
                use("%s").
                read (dense: true) $0-$%d select count(*) from recent "90m" end.
            """ % (sw, sensors - 1)
            (ok, r) = user.query(q)
            assert ok == "ok", r
            if args.verbose:
                print(q)
                print(r)
        elif args.query == 2:
            cb = 5
            d = []
            for sw in sws:
                (ok, r) = user.query('get_report("describe_swimlane", #{"key" => "%s"}).' % sw)
                assert ok == "ok", r
                d.append((r["utilized_sensors"], sw))
            d.sort(reverse=True)
            for (sensors, sw) in d:
                q = """
                    use("%s").
                    read (dense: true) $0-$%d select count(*) from recent "2H" end.
                """ % (sw, sensors - 1)
                (ok, r) = user.query(q)
                assert ok == "ok", r
                if args.verbose:
                    print("*" * 10)
                    print(q)
                    print(r)
                cb -= 1
                if cb == 0:
                    break
        else:
            sw = sws[0]
            (ok, r) = user.query('get_report("describe_swimlane", #{"key" => "%s"}).' % sw)
            assert ok == "ok", r
            sensors = r["utilized_sensors"]
            q = """
                use("%s").
                read (dense: true) $0-$%d select count(*) from recent "90m" end.
            """ % (sw, sensors - 1)
            (ok, r) = user.query(q)
            assert ok == "ok", r
            if args.verbose:
                print(q)
                print(r)
    else:
        print("unknown test scenario: %d" % args.test)


#############################################################################


def get_su(args):
    return Mdtsdb(
        host=utils.HOST,
        port=utils.PORT,
        admin_key=args.su_key,
        secret_key=args.su_secret,
        timeout=REQ_TIMEOUT,
        is_https=utils.ISHTTPS)


def main(args):
    r = None
    try:
        creds = open_creds(args, CREDS)
        if args.create:
            r = create(args, creds)
        elif args.env:
            r = update_env(args, creds)
        elif args.write:
            r = write(args, creds)
        elif args.validate:
            r = validate(args, creds)
        elif args.info:
            r = print_info(args, creds)
        elif args.size:
            r = print_size(args, creds)
        else:
            key = str(args.test)
            if key in creds:
                if args.delete:
                    r = clean(args, creds)
                else:
                    r = read_scenario(args, creds)
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
    parser.add_argument('-t', '--test', type=int, choices=range(1, 101), help='test scenario number', default=1)
    parser.add_argument('-c','--create', help='Create Prometheus User', required=False, action='store_true')
    parser.add_argument('-w','--write', help='Write data', required=False, action='store_true')
    parser.add_argument('-q', '--query', type=int, choices=range(1, 11), help="""Read scenario""", required=False, default=1)
    parser.add_argument('-v','--validate', help='Validate data', required=False, action='store_true')
    parser.add_argument('-d','--delete', help='Clean data', required=False, action='store_true')
    parser.add_argument('-i','--info', help='Print info about Prometheus User', required=False, action='store_true')
    parser.add_argument('-z','--size', type=int, help='Print summary about Prometheus User', required=False)
    parser.add_argument('-e','--env', help='Update User environment', required=False, action='store_true')
    parser.add_argument('-n', '--num', type=int, help="""Number of data points to write""", required=False, default=100)
    parser.add_argument('--filter', type=int, choices=range(0, 3), help="Generate data for filtering", required=False, default=1)
    parser.add_argument('--verbose', help='verbose: True or False', required=False, action='store_true', default=False)
    parser.add_argument('--creds', help="file with credential info", required=False)
    parser.add_argument('--user', help="Prometheus User", required=False, default="MyPromUser")
    parser.add_argument('--user_details', help="Prometheus User", required=False, default="Prometheus User")
    parser.add_argument('--su_key', help="Prometheus SU Key", required=False, default="MyPromOwnerUser")
    parser.add_argument('--su_secret', help="Prometheus SU Secret", required=False, default="MyPromOwnerSecret")

    args = parser.parse_args()

    if args.server != None:
        utils.HOST = args.server
    if args.port != None:
        utils.PORT = int(args.port)
    if args.use_https != None:
        utils.ISHTTPS = args.use_https

    try:
        main(args)
    except ConnectionError as e:
        print(e)

#############################################################################
