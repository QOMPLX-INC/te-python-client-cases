#!/usr/bin/python3
#
# inspect_prom.py - inspect Prometheus metrics
#
#

from __future__ import print_function
import argparse, os, sys, json, random, time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../common')))

from mdtsdb import Mdtsdb
import utils
from utils import (new_user, ConnectionError, HOST, PORT, REQ_TIMEOUT, ISHTTPS)

#############################################################################

def main(args):
    user_owner = Mdtsdb(
        host=utils.HOST,
        port=utils.PORT,
        admin_key=args.name,
        secret_key=args.password,
        timeout=REQ_TIMEOUT,
        is_https=utils.ISHTTPS)
    (Ok, r) = user_owner.query("""get_user_secret("%s").""" % args.user)
    assert Ok == 'ok', (Ok, r)
    user_as_storage = Mdtsdb(
        host=utils.HOST,
        port=utils.PORT,
        admin_key=args.user,
        secret_key=str(r),
        timeout=REQ_TIMEOUT,
        is_https=utils.ISHTTPS)
    t2 = int(time.time())
    t1 = t2 - int(args.dur)
    if args.value == "node_cpu_seconds_total":
        q = """
            L = select_metrics(%d, %d, [{"%s", "%s", '%s'}]),
            lists::filtermap(fun
                ({#{<<"instance">> := Instance}, Samples = [_ | _]}) ->
                    {true, {Instance, [CPU || {_, CPU, _} <- Samples]}};
                (_) ->
                    false
            end, L).
        """
    else:
        q = """
            select_metrics(%d, %d, [{"%s", "%s", '%s'}]).
        """
    q = q % (1000 * t1, 1000 * t2, args.metric, args.value, args.rule)
    print(q)
    (Ok, r) = user_as_storage.query(q)
    assert Ok == 'ok', (Ok, r)
    if args.value == "node_cpu_seconds_total":
        print(json.dumps(r, indent=4))
    else:
        print(r)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TimeEngine Python Client Test')
    parser.add_argument('-s','--server', help='TimeEngine server host', required=False)
    parser.add_argument('-p','--port', help='TimeEngine server port', required=False)
    parser.add_argument('--use_https', help='Use https scheme', required=False, default=False, action='store_true')
    parser.add_argument('--user', help="Prometheus User", required=False, default="MyPromUser")
    parser.add_argument('--name', help="Prometheus Owner User Key", required=False, default="MyPromOwnerUser")
    parser.add_argument('--password', help="Prometheus Owner User Secret", required=False, default="MyPromOwnerSecret")
    parser.add_argument('--metric', help="Metric name", required=False, default="__name__")
    parser.add_argument('--value', help="Metric value", required=False, default="node_cpu_seconds_total")
    parser.add_argument('--rule', help="PromQL rule: EQ, NEQ, RE, NRE", required=False, default="EQ", choices=["EQ", "NEQ", "RE", "NRE"])
    parser.add_argument('--dur', help="For the given number of seconds from now back", required=False, default=900, type=int)

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
