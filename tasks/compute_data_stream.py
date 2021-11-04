#!/usr/bin/python3
#
# compute_data_stream.py - server-side stateful computations over a data stream
#

import argparse, os, sys, csv, json, time, random

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../common')))
import utils
from utils import (new_user, new_swimlane,
                   write_num, KafkaConsumer,
                   ConnectionError,
                   create_clients, update_clients, open_creds,
                   clean, print_info)

CREDS = 'compute_data_stream.json'
TASK = """
task "detect_anomalies" (
    window: "tumbling",
    sz: 10,
    real_time: false,
    save: false, %% ephemeral results, no persistency on backend nodes
    options: #{
        "permanent": true
    },

    result: def (task_props, n, items) ->
        out = outliers(items; method: "mad", sensitivity: 0.1, format: "values"),
        sz_outliers = length(out),
        if
            sz_outliers > 0 ->
                msg = format("Found ~0p outliers (~0p) in a stream of ~0p items: ~0p. Data stream: '~s' (window: ~s, size: ~p).",
                    [sz_outliers, out, n, items, task_props.name, task_props.window, task_props.sz]),
                kafka msg
        end,
        out
    end
)
from [$0]
end.
"""

kafka_consumer = None
kafka_consumer_wait = 0
DEFAULT_KAFKA_TOPIC = 'compute_data_stream_test_notifications'

#############################################################################

def gen_data(serial_number, i, timestamp, sensor_no):
    if (serial_number + 1) % 7 == 0:
        v = random.randint(100, 199)
    else:
        v = random.randint(1, 10)
    return v

def init_kafka(args):
    global kafka_consumer, kafka_consumer_wait
    kafka_consumer = KafkaConsumer(
        args.kafka_server, args.kafka_port, args.kafka_topic,
        None, args.kafka_init_wait * 1000 + args.kafka_consumer_wait)
    kafka_consumer.start()
    kafka_consumer_wait = args.kafka_consumer_wait
    print("wait Kafka consumer...")
    time.sleep(args.kafka_init_wait)


def read_alarms():
    kafka_consumer.join(kafka_consumer_wait / 1000.0)
    msgs = kafka_consumer.msgs
    msgs_cb = len(msgs)
    if msgs_cb > 0:
        print("    there are %d messages from TimeEngine" % msgs_cb)
        for m in msgs:
            print(m)
    else:
        print("    no messages")


def write_main(args, creds):
    init_kafka(args)

    r = create_clients(args.test, creds)
    if r is not None:
        print("test scenario %d already exists" % args.test)
        return

    print("create test scenario: %d" % args.test)

    sw_opts = {
        'autoclean_off': True,
        'backend': False,
        'concurrent_write': False
    }
    user = new_user()
    swimlane = new_swimlane(user, sw_opts, {})
    print(sw_opts)

    (ok, r) = swimlane.query("""
        env kafka: #{
            "host": "%s",
            "port": %d,
            "topic": "%s",
            "options": #{"no_ack": true}
        } end.""" % (args.kafka_server, args.kafka_port, args.kafka_topic))
    assert ok == "ok", (ok, r)

    q = TASK
    print(q)
    (ok, r) = swimlane.query(q)
    assert ok == "ok", (ok, r)

    write_num(args, swimlane, args.blocks, args.sensors, args.pts, gen_data)
    (ok, r) = swimlane.insert([{'ns': int(time.time()) + 100, '0': 1}])
    assert ok == 'ok', (ok, r)

    read_alarms()

    update_clients(args.test, creds, user, swimlane, {})


#############################################################################

def main(args):
    r = None
    try:
        creds = open_creds(args, CREDS)
        if args.write:
            r = write_main(args, creds)
        elif args.info:
            r = print_info(args, creds)
        else:
            key = str(args.test)
            if key in creds:
                if args.delete:
                    r = create_clients(args.test, creds)
                    if r is None:
                        raise ValueError("unknown test scenario: %d" % args.test)
                    (user, swimlane, attrs) = r
                    (ok, r) = swimlane.query("""delete_task("detect_anomalies").""")
                    assert ok == "ok", (ok, r)
                    r = clean(args, creds)
                else:
                    print("read scenario is not supported: %d" % args.test)
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
    parser.add_argument('-d','--delete', help='Clean data', required=False, action='store_true')
    parser.add_argument('-i','--info', help='Print info about test scenario/swimlane', required=False, action='store_true')

    parser.add_argument('--blocks', type=int, help="number of blocks", default=10)
    parser.add_argument('--pts', type=int, help="number of points per block", default=10)
    parser.add_argument('--sensors', type=int, help="number of sensors", required=False, default=1)
    parser.add_argument('--read_back', help='Validate write by read back', required=False, action='store_true', default=False)
    parser.add_argument('--verbose', help='verbose: True or False', required=False, action='store_true', default=False)
    parser.add_argument('--creds', help="file with credential info", required=False)

    parser.add_argument('--kafka_server', help='Kafka host', required=False, default='localhost')
    parser.add_argument('--kafka_port', help='Kafka port', required=False, type=int, default=9092)
    parser.add_argument('--kafka_topic', help='Kafka topic (separated by ; if multiple)', required=False, default=DEFAULT_KAFKA_TOPIC)
    parser.add_argument('--kafka_read', help='Read from the topic', required=False, action='store_true')
    parser.add_argument('--kafka_consumer_wait', help='Wait for consume for milliseconds', required=False, type=int, default=5000)
    parser.add_argument('--kafka_init_wait', help='Wait for consumer to get ready for seconds', required=False, type=int, default=5)
    parser.add_argument('--kafka_points', help='Send N data points', required=False, type=int, default=11)
    parser.add_argument('--kafka_id', help='Kafka test ID (also the suffix of the Kafka topic)', required=False, default='')
    parser.add_argument('--kafka_ack', help='Request acknowledgments', required=False, type=int, choices=[0, 1], default=1)

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
    except KeyboardInterrupt:
        if consumer != None:
            consumer.join(args.kafka_consumer_wait / 1000.0)
        raise
    except ConnectionError as e:
        print(e)

#############################################################################
