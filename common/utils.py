#!/usr/bin/python3
#
# utils.py - create/delete User/Swimlane; CSV; store server info and auth records
#

from __future__ import print_function
from mdtsdb import Mdtsdb
from mdtsdb.exceptions import ConnectionError
import os, sys, json, csv, time, datetime, calendar

if sys.version_info >= (3,5,0):
    from _thread import *
else:
    from thread import *
import kafka, threading

HOST = "time-engine.qee.qomplxos.com"
PORT = 443
REQ_TIMEOUT = 600
MasterKey = "MyUser"
MasterSecret = "MySecret"
ISHTTPS = True

#############################################################################
# User/Swimlane

def new_user():
    super_user = Mdtsdb(
        host=HOST,
        port=PORT,
        admin_key=MasterKey,
        secret_key=MasterSecret,
        timeout=REQ_TIMEOUT,
        is_https=ISHTTPS)
    (Ok, r) = super_user.new_adminkey("User details")
    assert Ok == 'ok', (Ok, r)
    return Mdtsdb(
        host=HOST,
        port=PORT,
        admin_key=str(r['key']),
        secret_key=str(r['secret_key']),
        timeout=REQ_TIMEOUT,
        is_https=ISHTTPS)

def new_swimlane(user, swimlane_opts = [], client_options = {}):
    (Ok, r) = user.new_appkey("Swimlane details", swimlane_opts)
    assert Ok == 'ok', (Ok, r)
    cli = Mdtsdb(
        host=HOST,
        port=PORT,
        app_key=str(r['key']),
        secret_key=str(r['secret_key']),
        options = client_options,
        timeout=REQ_TIMEOUT,
        is_https=ISHTTPS)
    return cli

def del_user(user):
    super_user = Mdtsdb(
        host=HOST,
        port=PORT,
        admin_key=MasterKey,
        secret_key=MasterSecret,
        timeout=REQ_TIMEOUT,
        is_https=ISHTTPS)
    (Ok, r) = super_user.delete_adminkey(user.admin_key)
    assert Ok == 'ok' and 'status' in r and r['status'] == 1, (Ok, r)

def del_swimlane(user, swimlane):
    (Ok, r) = user.delete_appkey(swimlane.app_key)
    assert Ok == 'ok' and 'status' in r and r['status'] == 1, (Ok, r)

#############################################################################
# Store server info and auth records

def clean(args, creds):
    key = str(args.test)
    if key in creds:
        (user, swimlane, _) = create_clients(args.test, creds)
        print("clean %s " % creds[key])
        if swimlane:
            del_swimlane(user, swimlane)
        else:
            (ok, sws) = user.query("get_swimlanes().")
            assert ok == 'ok'
            for sw in sws:
                print(sw)
                (Ok, r) = user.delete_appkey(sw)
                assert Ok == 'ok' and 'status' in r and r['status'] == 1, (Ok, r)
        del_user(user)
        with open(creds['__path'], 'w') as fd:
            creds.pop(key, None)
            print(json.dumps(creds, indent=4), file=fd)
    else:
        print("unknown test scenario: %d" % args.test)
    return True

def print_info(args, creds):
    r = create_clients(args.test, creds)
    if r is not None:
        (user, swimlane, attrs) = r
        print(json.dumps(attrs, indent=4))
    else:
        print("unknown test scenario: %d" % args.test)

def create_clients(test_no, creds):
    key = str(test_no)
    if key in creds:
        attrs = creds[key]
        user = Mdtsdb(**{
            "host": HOST,
            "port": PORT,
            "admin_key": attrs['adm'],
            "secret_key": attrs['adm_secret'],
            "timeout": REQ_TIMEOUT,
            "is_https": ISHTTPS})
        if 'app' in attrs and 'app_secret' in attrs:
            swimlane = Mdtsdb(**{
                "host": HOST,
                "port": PORT,
                "app_key": attrs['app'],
                "secret_key": attrs['app_secret'],
                "timeout": REQ_TIMEOUT,
                "is_https": ISHTTPS})
        else:
            swimlane = None
        return (user, swimlane, attrs)
    else:
        return None

def update_clients(test_no, creds, user, swimlane, info):
    with open(creds['__path'], 'w') as fd:
        if swimlane:
            creds[str(test_no)] = {
                'adm': user.admin_key,
                'adm_secret': user.secret_key.decode(),
                'app': swimlane.app_key,
                'app_secret': swimlane.secret_key.decode()
            }
        else:
            creds[str(test_no)] = {
                'adm': user.admin_key,
                'adm_secret': user.secret_key.decode()
            }
        creds['info'] = info
        print(json.dumps(creds, indent=4), file=fd)

def creds_path(args, filename):
    if args.creds != None:
        creds_fn = args.creds
    else:
        creds_fn = os.path.join(os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__), '../data/')), filename)
    return creds_fn

def open_creds(args, filename):
    creds_fn = creds_path(args, filename)
    if not os.path.isfile(creds_fn):
        with open(creds_fn, 'w') as fd:
            print('{}', file=fd)
        creds = {}
    else:
        with open(creds_fn, 'r') as fd:
            creds = json.load(fd)
    creds['__path'] = creds_fn
    return creds

#############################################################################
# CSV

def csv_path(args, filename):
    if args.csv != None:
        return args.csv
    else:
        return os.path.join(os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__), '../data/')), filename)

def country_label(r):
    label = r[1]
    if r[0].strip() != '':
        label += '.' + r[0]
    return label

def import_csv(args, filename):
    cb = 0
    with open(csv_path(args, filename)) as f:
        rows = list(csv.reader(f))
        max_sensor = len(rows) - 1
        header, body = rows[0], rows[1:]
        no, labels, stationary, data = 0, {}, {}, []
        for r in header[4:]:
            data.append({
                'ns': calendar.timegm(datetime.datetime.strptime(r, "%m/%d/%y").timetuple())
            })
        for r in body:
            sensor = str(no)
            label = country_label(r)
            labels[sensor] = label
            stationary[sensor] = {'lat': float(r[2]), 'lng': float(r[3])}
            no += 1
            i = 0
            for value in r[4:]:
                data[i][sensor] = int(value)
                i += 1

    return header, body, max_sensor, labels, stationary, data

#############################################################################
# Numerical data: write

def write_num(args, swimlane, packs, sensors, n, gen_f = None, delay_f = None):
    t0 = (int(time.time()) - n * packs) // 10 * 10
    ti = t0
    cb = 0
    for pack in range(packs):
        data = []
        if args.verbose:
            print("pack: %d" % pack)
        for i in range(n):
            p = {str(no): gen_f(cb, i, ti, no) if gen_f else (i + 1) * (no + 1) for no in range(sensors)}
            p['ns'] = ti
            ti += 1
            data.append(p)
            cb += 1
        #print(data)
        (ok, r) = swimlane.insert(data)
        assert ok == 'ok', (ok, r)
        if args.verbose:
            print("Server write details:")
            print(json.dumps(r, indent=4))
        try:
            print("server write time: %d ms" % r['batch'][swimlane.app_key]['result']['info']['ms'])
        except KeyError:
            pass
        if delay_f:
            delay_f(pack)
    t2 = ti
    return t0, t2

#############################################################################
# Kafka

class KafkaConsumer(threading.Thread):
    def __init__(self, kafka_server, kafka_port, kafka_topic, n_msgs, wait_ms):
        threading.Thread.__init__(self)
        self.kafka_server = kafka_server
        self.kafka_port = kafka_port
        self.kafka_topic = kafka_topic
        self.n_msgs = n_msgs
        self.wait_ms = wait_ms
        self.msgs = []

    def run(self):
        kafka_topics = self.kafka_topic.split(';')
        if len(kafka_topics) == 1:
            consumer = kafka.KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers='%s:%d' % (self.kafka_server, self.kafka_port),
                consumer_timeout_ms=self.wait_ms)
        else:
            consumer = kafka.KafkaConsumer(
                bootstrap_servers='%s:%d' % (self.kafka_server, self.kafka_port),
                consumer_timeout_ms=self.wait_ms)
            consumer.subscribe(topics = kafka_topics)
        cb = 0
        for msg in consumer:
            self.msgs.append(msg.value)
            #self.msgs.append("%s %d" % (msg.topic, msg.offset))
            cb += 1
            if self.n_msgs and cb >= self.n_msgs:
                break
        consumer.close()

    def topics():
        return kafka.KafkaConsumer(bootstrap_servers=['%s:%d' % (self.kafka_server, self.kafka_port)]).topics()

#############################################################################
