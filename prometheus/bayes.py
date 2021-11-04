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

BAYES_MODEL_FILE = 'bayes_model.txt'
with open(os.path.join(os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__))), BAYES_MODEL_FILE), 'r') as fd:
    BAYES_MODEL = fd.read()

TASK_BODY = """
%%-------------------------------------------------------------------
%% Model:
BayesModel = """ + BAYES_MODEL + """

%% Bayesian network nodes graph
Graph = #{
    0: [1, 2, 3, 4],
    1: [5, 6],
    2: [5, 6],
    3: [5, 6],
    4: [5, 6]
},

%% Nodes max values [0..Max):
Upper = [2, 3, 5, 5, 5, 2, 2].

%%-------------------------------------------------------------------

fun
scale(1, Value) ->
    if
        Value =< 50 -> 0;
        Value =< 500 -> 1;
        true -> 2
    end;
scale(2, Value) ->
    if
        Value =< 0.10 -> 0;
        Value =< 0.25 -> 1;
        Value =< 0.40 -> 2;
        Value =< 0.55 -> 3;
        true -> 4
    end;
scale(3, Value) ->
    if
        Value =< 0.1 -> 0;
        Value =< 0.2 -> 1;
        Value =< 0.3 -> 2;
        Value =< 0.4 -> 3;
        true -> 4
    end;
scale(4, Value) ->
    if
        Value =< 0.001 -> 0;
        Value =< 0.01 -> 1;
        Value =< 0.1 -> 2;
        Value =< 0.5 -> 3;
        true -> 4
    end.

fun classify(<<"%(ping)s">>) -> 1;
    classify(<<"%(cpu)s">>) -> 2;
    classify(<<"%(mem)s">>) -> 3;
    classify(<<"%(reject)s">>) -> 4.

fun extract_prob(#{0 := [_, Pcrash], 5 := [_, Pddos], 6 := [_, Poverload]}) ->
    {Pcrash, Pddos, Poverload}.

%%-------------------------------------------------------------------

use("%(ping)s").
task "observe_mdtsdb_nodes" (
    window: "sliding",
    slide: 60,
    slides_per_window: 10,

    real_time: true,
    save: false,

    options: #{
        "report_data_drop": true,
        "report_insert_drop": true,
        "permanent": true
    },

    partition: def (swimlane, sensor_no, _, _) ->
        case get_sensor_label(swimlane, sensor_no) of
            #{"instance" => instance} ->
                instance;
            name ->
                name
        end
    end,

    map: def (swimlane, _, timestamp, value) ->
        {classify(swimlane), {timestamp div 1_000_000_000, value}}
    end,

    result: def (task_props, n, series) ->
        %% map type (1-4) to a list of observed values for this type of system metrics
        series = to_map_append(series),
        min_n = case n of
            0 ->
                0;
            _ ->
                maps::fold(fun (_, L, Acc) -> min(length(L), Acc) end, 'undefined', series)
        end,
        if
            maps::size(series) < 4 ->
                'null';
            min_n > 0 ->
                t_now = now(),
                %% current state of an instance as type to average scaled (0-2 and 0-4) value for this type of system metrics
                bayes_state = maps::map(def (metric_no, points) ->
                    {_, latest_values} = lists::unzip(lists::sublist(points, max(1, length(points) - 4), 5)),
                    scaled = lists::map(def (value) -> scale(metric_no, value) end, latest_values),
                    round(avg(scaled))
                end, series),
                %% map risk type to current event probability
                p_now = bayes_evaluate_model(Graph, Upper, BayesModel, bayes_state, 5000),
                %% prediction (+1 minute) of an instance state
                t_predict = t_now + 60,
                bayes_state_prediction = maps::map(def (metric_no, points) ->
                    {times, values} = lists::unzip(points),
                    [value_predict] = svr_predict([times, values]; gamma: 0.125, c: 20, tolerance: 0.001, t: [t_predict]),
                    round(scale(metric_no, value_predict))
                end, series),
                %% map risk type to future event probability
                p_future = bayes_evaluate_model(Graph, Upper, BayesModel, bayes_state_prediction, 5000),
                {t_now, extract_prob(p_now), t_predict, extract_prob(p_future)};
            n > 0 ->
                'null';
            true ->
                'null'
        end
    end,

    collect_by_key: def (task_props, t1, t2, partition_key, result) ->
        case result of
            {t_now, {p_crash, p_ddos, p_overload}, t_predict, {p_crash2, p_ddos2, p_overload2}} ->
                t_now_ms = t_now * 1000,
                labels = #{
                    "__name__": "%(node_metric)s",
                    "instance": partition_key
                },
                series = [
                    {labels#{"risk": "crash",    "when": "now"},    [{t_now_ms, p_crash}]},
                    {labels#{"risk": "ddos",     "when": "now"},    [{t_now_ms, p_ddos}]},
                    {labels#{"risk": "overload", "when": "now"},    [{t_now_ms, p_overload}]},
                    {labels#{"risk": "crash",    "when": "future"}, [{t_now_ms, p_crash2}]},
                    {labels#{"risk": "ddos",     "when": "future"}, [{t_now_ms, p_ddos2}]},
                    {labels#{"risk": "overload", "when": "future"}, [{t_now_ms, p_overload2}]}
                ],
                send_metrics(series),
                result;
            _ ->
                result
        end
    end
)
from %(from)s
end.

%%-------------------------------------------------------------------

def classify(series) ->
    %% density-based spatial clustering of (DDoS, Overload) risk probabilities with noise
    {ddos, overload} = lists::unzip(maps::values(series)),
    class_labels = dbscan([overload, ddos]; r: 0.2, n: 3),
    class_centroids = lists::foldl(fun
        ({0, _, _}, Map) -> %% noise
            Map;
        ({No, D, O}, Map) ->
            case Map of
                #{No := {L1, L2}} ->
                    Map#{No := {[D | L1], [O | L2]}};
                _ ->
                    Map#{No => {[D], [O]}}
            end
    end, #{}, lists::zip3(class_labels, ddos, overload)),
    %% build centroids: {Mean DDoS prob, Mean overload prob} => number of nodes in the risk cluster
    lists::keysort(1, maps::fold(fun (_, {Ds, Os}, Acc) ->
        Sz = length(Ds),
        [{Sz, mdtsdb:format("(~.2f, ~.2f)", [lists:sum(Ds) / Sz, lists:sum(Os) / Sz])} | Acc]
    end, [], class_centroids))
end.


use("%(cluster_report_swimlane)s").
task "observe_mdtsdb_cluster" (
    window: "tumbling",
    sz: 60,

    real_time: true,
    save: false,

    options: #{
        "report_data_drop": true,
        "report_insert_drop": true,
        "permanent": true
    },

    filter: def (swimlane, sensor_no, _, value) ->
        case get_sensor_label(swimlane, sensor_no) of
            #{"risk": "ddos", "when": "now", "instance" => instance} ->
                {true, {instance, 'ddos', value}};
            #{"risk": "overload", "when": "now", "instance" => instance} ->
                {true, {instance, 'overload', value}};
            _ ->
                false
        end
    end,

    result: def (task_props, n, series) ->
        %% prepare source data
        series = lists::foldl(fun
            ({Instance, 'ddos', Value}, Acc) ->
                case Acc of
                    #{Instance := {DDoS, Overload}} ->
                        Acc#{Instance := {max(DDoS, Value), Overload}};
                    _ ->
                        Acc#{Instance => {Value, -1}}
                end;
            ({Instance, 'overload', Value}, Acc) ->
                case Acc of
                    #{Instance := {DDoS, Overload}} ->
                        Acc#{Instance := {DDoS, max(Overload, Value)}};
                    _ ->
                        Acc#{Instance => {-1, Value}}
                end
        end, #{}, series),
        %% preprocess: one pass would mean more obfuscated and longer code
        series = maps::filter(fun (_, {DDoS, Overload}) ->
            DDoS >= 0.0 and Overload >= 0.0
        end, series),
        {class2, class3, class4, n_threat, n_ddos, n_total} = maps::fold(fun
            (Instance, {DDoS, Overload}, {C2, C3, C4, NT, ND, N}) when DDoS < 0.3, Overload < 0.3 ->
                {C2, C3, C4, NT, ND, 1 + N};
            (Instance, {DDoS, Overload}, {C2, C3, C4, NT, ND, N}) when DDoS < 0.3 ->
                {[Instance | C2], C3, C4, 1 + NT, ND, 1 + N};
            (Instance, {DDoS, Overload}, {C2, C3, C4, NT, ND, N}) when Overload < 0.3 ->
                {C2, [Instance | C3], C4, 1 + NT, 1 + ND, 1 + N};
            (Instance, {DDoS, Overload}, {C2, C3, C4, NT, ND, N}) ->
                {C2, C3, [Instance | C4], 1 + NT, 1 + ND, 1 + N}
        end, {[], [], [], 0, 0, 0}, series),
        %% classify
        if
            n_threat == 0 ->
                %% nothing to report
                0;
            n_total > 0 ->
                attack_broadness = n_ddos / n_total,
                %% custom alert: report to Kafka if risk is higher than the threshold
                if
                    attack_broadness > 0.1 ->
                        %% density-based spatial clustering of (DDoS, Overload) risk probabilities with noise
                        class_centroids = classify(series),
                        %% report
                        fmt = "Risks: overloaded(~0p)=~0p, ddos(~0p)=~0p, both(~0p)=~0p.~nRisk clusters: ~0p.~nTask: '~s' ('~s' ~ps)",
                        report = format(fmt, [length(class2), class2, length(class3), class3, length(class4), class4,
                                              class_centroids, task_props.name, task_props.window, task_props.sz])
                        %% kafka (topic: "mdtsdb_cluster_eval_status") report
                end,
                %% result is "attack broadness"
                attack_broadness
        end
    end,

    collect: def (task_props, t1, t2, attack_broadness) ->
        %% side effect: publish cluster status as another Prometheus metrics
        send_metric(#{"__name__": "%(cluster_metric)s", "type": "ddos"}, [{now() * 1000, attack_broadness}])
    end
)
from ["%(report_swimlane)s".$0-$6000]
end.

"""

METRIC_PING   = "mdtsdb_ping_ms"
METRIC_CPU    = "mdtsdb_cpu_rel"
METRIC_MEM    = "mdtsdb_mem_db_node_rel"
METRIC_REJECT = "mdtsdb_gen_reject"

METRIC_NODE_EVAL_STATUS = "mdtsdb_node_eval_status"
METRIC_CLUSTER_EVAL_STATUS = "mdtsdb_cluster_eval_status"

#############################################################################


def ensure_source_swimlane(user, metric):
    (ok, sw) = user.query("""tags_to_swimlane(#{"__name__": "%s"}).""" % metric)
    assert ok == 'ok', (ok, sw)
    (ok, opts) = user.query("""get_swimlane_opts("%s").""" % sw)
    assert ok == 'ok', (ok, opts)
    return (sw, opts["labels"])


def ensure_report_swimlane(user, metric):
    (ok, sw) = user.query("""create_prom_swimlane(#{"__name__": "%s"}).""" % metric)
    assert ok == 'ok', (ok, sw)
    (ok, opts) = user.query("""get_swimlane_opts("%s").""" % sw)
    assert ok == 'ok', (ok, opts)
    return (sw, opts["labels"])


def config_swimlane(user, task_body, params):
    (ok, r) = user.query("""get_task("observe_mem").""")
    assert ok == 'ok', (ok, r)
    if isinstance(r, dict):
        print("User Task already exists:\n%s" % json.dumps(r, indent=4))
    else:
        task_body = task_body % params
        print("%s" % task_body)
        (ok, r) = user.query(task_body)
        assert ok == 'ok', (ok, r)


def create(args, creds):
    r = create_clients(args.test, creds)
    if r is not None:
        (user, _, attrs) = r
        print(json.dumps(attrs, indent=4))

        (report_sw, report_labels) = ensure_report_swimlane(user, METRIC_NODE_EVAL_STATUS)
        print("touch node report swimlane: %s: %s" % (report_sw, report_labels))
        (cluster_report_sw, cluster_report_labels) = ensure_report_swimlane(user, METRIC_CLUSTER_EVAL_STATUS)
        print("touch cluster report swimlane: %s: %s" % (cluster_report_sw, cluster_report_labels))

        (ping_sw, ping_labels) = ensure_source_swimlane(user, METRIC_PING)
        print("found ping swimlane: %s: %s" % (ping_sw, ping_labels))
        (mem_sw, mem_labels) = ensure_source_swimlane(user, METRIC_MEM)
        print("found mem swimlane: %s: %s" % (mem_sw, mem_labels))
        (cpu_sw, cpu_labels) = ensure_source_swimlane(user, METRIC_CPU)
        print("found cpu swimlane: %s: %s" % (cpu_sw, cpu_labels))
        (rj_sw, rj_labels) = ensure_source_swimlane(user, METRIC_REJECT)
        print("found reject swimlane: %s: %s" % (rj_sw, rj_labels))

        if len(mem_labels) > 0 and len(cpu_labels) > 0 and len(ping_labels) > 0 and len(rj_labels) > 0:
            data_source = """["%s".$0-$%d, "%s".$0-$%d, "%s".$0-$%d, "%s".$0-$%d]""" % (
                ping_sw, len(ping_labels) - 1,
                mem_sw, len(mem_labels) - 1,
                cpu_sw, len(cpu_labels) - 1,
                rj_sw, len(rj_labels) - 1
            )
            params = {
                "from": data_source,
                "ping": ping_sw,
                "cpu": cpu_sw,
                "mem": mem_sw,
                "reject": rj_sw,
                "report_swimlane": report_sw,
                "cluster_report_swimlane": cluster_report_sw,
                "node_metric": METRIC_NODE_EVAL_STATUS,
                "cluster_metric": METRIC_CLUSTER_EVAL_STATUS
            }
            config_swimlane(user, TASK_BODY, params)
        else:
            print("miss ping/memory/cpu/reject metrics: %d" % args.test)
    else:
        print("unknown test scenario: %d" % args.test)


def clean(args, creds):
    key = str(args.test)
    if key in creds:
        (user, _, _) = create_clients(args.test, creds)
        print("clean tasks in %s" % creds[key])

        (ok, r) = user.query("""delete_swimlane(tags_to_swimlane(#{"__name__": "%s"})).""" % METRIC_NODE_EVAL_STATUS)
        print("delete swimlane of the metric %s: %s" % (METRIC_NODE_EVAL_STATUS, ok))

        (ok, r) = user.query("""delete_swimlane(tags_to_swimlane(#{"__name__": "%s"})).""" % METRIC_CLUSTER_EVAL_STATUS)
        print("delete swimlane of the metric %s: %s" % (METRIC_CLUSTER_EVAL_STATUS, ok))

        (ok, tasks) = user.query("user_tasks().")
        assert ok == 'ok', (ok, tasks)
        for task in tasks:
            print("delete task '%s'" % task)
            (ok, r) = user.query("""delete_task("%s").""" % task)
            assert ok == 'ok', (ok, r)
    else:
        print("unknown test scenario: %d" % args.test)

    return True


def print_info(args, creds):
    r = create_clients(args.test, creds)
    if r is not None:
        (user, _, attrs) = r
        print(json.dumps(attrs, indent=4))

        (ok, tasks) = user.query("user_tasks().")
        assert ok == 'ok', (ok, tasks)
        for task in tasks:
            print(task)
            if args.verbose:
                (ok, task_record) = user.query("""get_task("%s").""" % task)
                assert ok == 'ok', (ok, task_record)
                print(json.dumps(task_record, indent=4, sort_keys=True))
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


def main(args):
    r = None
    try:
        creds = open_creds(args, CREDS)
        if args.create:
            r = create(args, creds)
        elif args.env:
            r = update_env(args, creds)
        elif args.info:
            r = print_info(args, creds)
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
    parser.add_argument('--user', help='TimeEngine User key', required=False)
    parser.add_argument('--secret', help='TimeEngine User secret', required=False)
    parser.add_argument('-t', '--test', type=int, choices=range(1, 11), help='test scenario number', default=1)
    parser.add_argument('-c','--create', help='Create Prometheus User', required=False, action='store_true')
    parser.add_argument('-q', '--query', type=int, choices=range(1, 11), help="""Read scenario""", required=False, default=1)
    parser.add_argument('-d','--delete', help='Clean data', required=False, action='store_true')
    parser.add_argument('-i','--info', help='Print info about test scenario/swimlane', required=False, action='store_true')
    parser.add_argument('-e','--env', help='Update User environment', required=False, action='store_true')
    parser.add_argument('-n', '--num', type=int, help="""Number of data points to write""", required=False, default=100)
    parser.add_argument('--filter', type=int, choices=range(0, 3), help="Generate data for filtering", required=False, default=1)
    parser.add_argument('--verbose', help='verbose: True or False', required=False, action='store_true', default=False)
    parser.add_argument('--creds', help="file with credential info", required=False)
    parser.add_argument('--user', help="Prometheus User", required=False, default="MyPromUser")
    parser.add_argument('--su_key', help="Prometheus SU Key", required=False, default="MyPromOwnerUser")
    parser.add_argument('--su_secret', help="Prometheus SU Secret", required=False, default="MyPromOwnerSecret")

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
