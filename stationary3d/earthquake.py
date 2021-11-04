#!/usr/bin/python3
#
# earthquake.py - storing earthquake historical records as a 3-dimensional grid, and forecast individual 3-d grid states
# ./stationary3d/earthquake.py -q 6 -t 1 --resample_use_m
# [34.7, 32.6, 30.5, 29.1, 28.5, 28.2, 28.2, 28.2, 28.2, 28.2, 28.2, 28.2]
# ./stationary3d/earthquake.py -q 4 -t 1 --resample_use_m
# [27.7, 31.6, 27.8, 29.0, 28.1, 28.0, 28.1, 27.8, 27.9, 27.7, 27.8, 27.7]
# ./stationary3d/earthquake.py -q 5 -t 1 --resample_use_m
# [35.4, 35.6, 35.6, 35.7, 35.7, 35.6, 35.6, 35.6, 35.5, 35.5, 35.5, 35.4]
#
#

import argparse, os, sys, csv, json, time, calendar, datetime, operator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../common')))
import utils
from utils import (new_user, new_swimlane,
                   ConnectionError,
                   create_clients, update_clients, open_creds,
                   clean, print_info,
                   csv_path, HOST, PORT, ISHTTPS)

CREDS = 'earthquake.json'
CSV = 'earthquake_time_series.csv'

MEASUREMENT = "m1"
SCALE = 6
USER_ENV = """
Scale = %d,
AltStep = 35,
user env (
    measurements: #{
        "%s": #{
            "opts": #{
                "time_slice": 864000,
                "autoclean_off": true
            },
            "partition": #{
                "get_swimlane": fun (#{<<"site">> := Site}, _KeyOpts, AdminKey) ->
                    mdtsdb:format("~s_~s", [AdminKey, Site])
                end,
                "get_sensor": fun (#{<<"lat">> := Lat, <<"lng">> := Lng, <<"alt">> := Alt0}) ->
                    Hash = mdtsdb:geohash_encode(Lat, Lng, Scale),
                    Alt = round(Alt0) div AltStep * AltStep,
                    mdtsdb:format("C~pZ~p", [Hash, Alt])
                end
            }
        }
    }
) end,

get_report("measurements").
""" % (SCALE, MEASUREMENT)
SITE = "tr"

SET_GEO_POS1 = """
fun label2pos(<<"C", Rest/binary>>) ->
    [Hash | _] = binary:split(Rest, <<"Z">>),
    mdtsdb:geohash_decode(Hash, %d).
""" % SCALE

SET_GEO_POS2 = """
stationary = maps::map(def (_, name) ->
    {lat1, lat2, lng1, lng2} = label2pos(name),
    #{
        "type": "Polygon",
        "coordinates": [
            [
                [lng1, lat1],
                [lng2, lat1],
                [lng2, lat2],
                [lng1, lat2],
                [lng1, lat1]
            ]
        ]
    }
end, get_sensors_to_labels()).

env
    geo_position: stationary
end.
"""

LIM_RECORDS = 2000000

#############################################################################

def import_csv(args, filename):
    cb, data = 0, []
    with open(csv_path(args, filename)) as f:
        rows = list(csv.reader(f, delimiter='\t'))
        header, body = rows[0], rows[1:]
        for r in body:
            dt = r[2] + "T" + r[3]
            ns = calendar.timegm(datetime.datetime.strptime(dt, "%Y.%m.%dT%H:%M:%S.%f").timetuple())
            if r[13] == "Ke":
                cb += 1
                data.append({
                    "ns": ns,
                    "value": {
                        "m": float(r[7]),
                        "lat": float(r[4]),
                        "lng": float(r[5]),
                        #"alt": - float(r[6])
                        "alt": round(abs(float(r[6])))
                    },
                    "series": {
                        "m": float(r[7]),
                        "lat": float(r[4]),
                        "lng": float(r[5]),
                        #"alt": - float(r[6])
                        "alt": round(abs(float(r[6])))
                    }
                })
            if cb > LIM_RECORDS:
                break
    #print("read %d records" % len(data))
    return data

def write(args, creds):
    write1(args, creds)

def write1(args, creds):
    ms0 = time.time()
    r = create_clients(args.test, creds)
    if r is not None:
        print("test scenario %d already exists" % args.test)
        return

    print("create test scenario: %d" % args.test)
    data = import_csv(args, CSV)

    user = new_user()
    if args.verbose:
        print(USER_ENV)
    (Ok, r) = user.query(USER_ENV)
    assert Ok == 'ok', (Ok, r)
    if args.verbose:
        print(json.dumps(r, indent=4, sort_keys=True))

    ims0 = time.time()
    start, sz, sent = 0, 10000, 0
    part = {'site': SITE}
    while True:
        payload = data[start:start+sz]
        payload_sz = len(payload)
        if payload_sz == 0:
            break
        print("sending data[%d-%d)" % (start, start + sz))
        (Ok, r) = user.insert([{
            'measurement': MEASUREMENT,
            'series': part,
            'data': payload
        }])
        #if args.verbose:
        #    print(Ok, json.dumps(r, indent=4, sort_keys=True))
        assert Ok == 'ok' and 'status' in r and r['status'] == 1, (Ok, json.dumps(r, indent=4, sort_keys=True))
        sent += payload_sz
        start += sz
    ims1 = time.time()
    print("sent %d records" % sent)

    if args.verbose:
        print("Server write details:")
        print(json.dumps(r, indent=4, sort_keys=True))
    try:
        for _, v in r['batch'].items():
            print("server write time: %d ms" % v['result']['info']['ms'])
    except KeyError:
        pass

    if args.verbose:
        (ok, r) = user.query('get_swimlane_opts("%s_%s").' % (user.admin_key, SITE))
        print("Swimlane options:")
        r["labels"] = len(r["labels"])
        print(json.dumps(r, indent=4, sort_keys=True))

    upper_sensor, sws = validate1(args, user)
    update_clients(args.test, creds, user, None, {str(args.test): {'upper_sensor': upper_sensor}})

    print("OK: Data are written, scenario: %d, elapsed: %ss (sending: %ss)" % (
        args.test, round((time.time() - ms0) * 1000) / 1000.0, round((ims1 - ims0) * 1000) / 1000.0))

    if args.geo:
        write_geo_pos(args, user, sws)


def write_geo_pos(args, user, sws):
    for sw in sws:
        q = """%s\nuse("%s"),\n%s""" % (SET_GEO_POS1, sw, SET_GEO_POS2)
        if args.verbose:
            print(q)
        (ok, r) = user.query(q)
        assert ok == 'ok', (ok, r)

        if args.verbose:
            q = """get_sensor_positions("%s").""" % sw
            (ok, r) = user.query(q)
            print("Create geo-stationary sensors:")
            r2 = []
            for str_no, pos in r.items():
                r2.append((int(str_no), json.dumps(pos, indent=4, sort_keys=True)))
            r2.sort()
            for no, pos in r2:
                print("no: %d, pos: %s" % (no, pos))

    return True


def read_scenario(args, creds):
    r = create_clients(args.test, creds)
    if r is None:
        raise ValueError("unknown test scenario: %d" % args.test)

    (user, _, attrs) = r
    upper_sensor = creds['info'][str(args.test)]['upper_sensor']

    (ok, sws) = user.query("get_swimlanes().")
    assert ok == 'ok'
    print("Created swimlanes:")
    for sw in sws:
        print(sw)

    for sw in sws:
        if args.query == 1:
            r = read_by_alt(args, user, sw, upper_sensor)
        elif args.query == 2:
            r = read_ADF_test(args, user, sw, upper_sensor)
        elif args.query == 3:
            r = read_resample_test(args, user, sw, upper_sensor)
        elif args.query == 4:
            r = read_ARMA_test(args, user, sw, upper_sensor)
        elif args.query == 5:
            r = read_AR_test(args, user, sw, upper_sensor)
        elif args.query == 6:
            r = read_SVR_test(args, user, sw, upper_sensor)
        elif args.query == 7:
            r = read_resample_geo_test(args, user, sw, upper_sensor)
        elif args.query == 8:
            r = read_AR_geo_test(args, user, sw, upper_sensor)
        else:
            raise ValueError("Unknown read scenario: %d" % args.query)

        if True: #args.verbose:
            if args.query == 3 or args.query == 7:
                for col in r:
                    print(col.values())
            elif isinstance(r, dict):
                print("Server read details:")
                print(json.dumps(r, indent=4, sort_keys=True))
                print("server read time: %d ms" % r['data'][0]['ms'])
            elif isinstance(r, list):
                for vec in r:
                    if isinstance(vec, list):
                        print(map(lambda x: round(x * 10) / 10, vec))
                    elif isinstance(vec, dict) and 'forecast' in vec:
                        print(map(lambda x: round(x * 10) / 10, vec['forecast']))
                    else:
                        print(vec)
            else:
                print(r)

def read_by_alt(args, user, sw, upper_sensor):
    (ok, r) = user.query("""
fun merge_cols(_Xs, Ys) ->
    [begin
        case [Magnitude || #{<<"m">> := Magnitude} <- Group] of
            [] ->
                #{
                    <<"alt">> => Alt,
                    <<"count">> => 0,
                    <<"max">> => 'null',
                    <<"min">> => 'null',
                    <<"avg">> => 'null'
                };
            Values ->
                Sz = length(Values),
                #{
                    <<"alt">> => Alt,
                    <<"count">> => Sz,
                    <<"max">> => lists:max(Values),
                    <<"min">> => lists:min(Values),
                    <<"avg">> => lists:sum(Values) / Sz
                }
        end
    end || #{<<"alt">> := Alt, 'group' := Group} <- Ys].

use("%s"),
read $0-$%d
    select orderby($w; ref: $.alt)
    group * by $.alt as w
    filter $w by merge_cols
    format json (array: true)
end.
    """ % (sw, upper_sensor - 1))
    assert ok == 'ok' and 'data' in r and 'values' in r['data'][0], (ok, r)
    dataset = r['data'][0]['values']

    for sensor, values in dataset.items():
        pass

    if args.verbose:
        print("Server read details:")
        print(json.dumps(r, indent=4, sort_keys=True))
        print("server read time: %d ms" % r['data'][0]['ms'])

    return True


def read_ADF_test(args, user, sw, upper_sensor):
    q = """
def adf(cols) ->
    i = -1,
    map(def (x) ->
        sz = length(x),
        i = i + 1,
        feature = if
            sz > %d ->
                #{
                    \"adfstat\" := ADFStat,
                    \"pvalue\" := PValue,
                    \"critvalues\" := CritValues
                } = adfuller(x; maxlag: 0, regression: \"c\", autolag: \"aic\"),
                if
                    PValue < 0.05 ->
                        "stationary (by significance level)";
                    lists::all(fun (CritValue) -> ADFStat > CritValue end, CritValues) ->
                        "--- non-stationary ---";
                    true ->
                        "stationary (by critical values)"
                end;
            true ->
                "small dataset"
        end,
        format("~s (no. ~p), size ~p: ~s", [get_sensor_label(i), i, sz, feature])
    end, cols)
end.

use("%s"),
select adf($0-$%d.m)
    where cnt > %d when cnt = count($)
end.
    """ % (args.resample_min_records, sw, upper_sensor, args.resample_min_records)
    print(q)
    (ok, r) = user.query(q)
    assert ok == 'ok' and 'data' in r and 'values' in r['data'][0], (ok, r)

    if args.verbose:
        print("Server read details:")
        print(json.dumps(r, indent=4, sort_keys=True))
        print("server read time: %d ms" % r['data'][0]['ms'])

    return True


def read_resample_script(args, sw, upper_sensor):
    q = """
use("%s"),
datas = select $0 %%$0-$%d
    from "2010-01-01T00:00:00+00:00" to "2020-06-30T00:00:00+00:00"
    where cnt > %d when cnt = count($)
    resample by "%dd" ?fun (Values0, Acc) ->
        {Sum0, Dt0} = case Acc of
            'null' ->
                {0, 0};
            {_, _} ->
                Acc
        end,
        %s
        S = Sum0 + lists:sum(Values),
        Dt = Dt0 + 1,
        {round(1000.0 * S / Dt) / 1000.0, {S, Dt}}
    end
end.
    """ % (sw, upper_sensor - 1, args.resample_min_records, args.resample_days,
        """{_, Values1} = lists:unzip(Values0),
        Values = [Magnitude || #{<<"m">> := Magnitude} <- Values1],"""
        if args.resample_use_m else """Values = [length(Values0)],""")
    return q


def read_resample_test(args, user, sw, upper_sensor):
    q = read_resample_script(args, sw, upper_sensor)
    q = q + """
to_datapoints(datas).
    """
    if args.verbose:
        print(q)
    (ok, r) = user.query(q)
    assert ok == 'ok', (ok, r)
    return r


def read_ARMA_test(args, user, sw, upper_sensor):
    q = read_resample_script(args, sw, upper_sensor)
    q = q + """\n
map(def (data) ->
    am = arma_model(value(data); p: %d, q: %d),
    value(arma_forecast(data; params: am, n: %d, alpha: 0.05))
end, datas).
    """ % (args.model_p, args.model_q, args.model_n)
    if args.verbose:
        print(q)
    (ok, r) = user.query(q)
    assert ok == 'ok', (ok, r)
    return r


def read_AR_test(args, user, sw, upper_sensor):
    q = read_resample_script(args, sw, upper_sensor)
    q = q + """\n
map(def (data) ->
    am = ar_model(value(data); estimate: "ls", p: %d, const: false),
    value(ar_forecast(data; params: am, n: %d, alpha: 0.05))
end, datas).
    """ % (args.model_p, args.model_n)
    if args.verbose:
        print(q)
    (ok, r) = user.query(q)
    assert ok == 'ok', (ok, r)
    return r


def read_SVR_test(args, user, sw, upper_sensor):
    q = read_resample_script(args, sw, upper_sensor)
    q = q + """\n
map(def (data) ->
    sz = length(data),
    ts = lists::seq(1, sz),
    t_predict = lists::seq(sz + 1, sz + %d),
    svr_predict([ts, data]; gamma: 0.125, c: 20, tolerance: 0.001, t: t_predict)
end, value(datas)).
    """ % args.model_n
    if args.verbose:
        print(q)
    (ok, r) = user.query(q)
    assert ok == 'ok', (ok, r)
    return r


def read_resample_geo_script(args, sw, upper_sensor):
    q = """
use("%s"),

datas = read $0-$%d select $area
    box [35.43, 25.03, 42.79, 45.51]
    from "2010-01-01T00:00:00+00:00" to "2020-06-30T00:00:00+00:00"
    column (replace: true)
        merge(*) as area
    resample by "%dd" ?fun (Values0, Acc) ->
        {Sum0, Dt0} = case Acc of
            'null' ->
                {0, 0};
            {_, _} ->
                Acc
        end,
        %s
        S = Sum0 + lists:sum(Values),
        Dt = Dt0 + 1,
        {round(1000.0 * S / Dt) / 1000.0, {S, Dt}}
    end
end.
    """ % (sw, upper_sensor - 1, args.resample_days,
        """{_, Values1} = lists:unzip(Values0),
        Values = [Magnitude || #{<<"m">> := Magnitude} <- Values1],"""
        if args.resample_use_m else """Values = [length(Values0)],""")
    return q


def read_resample_geo_test(args, user, sw, upper_sensor):
    q = read_resample_geo_script(args, sw, upper_sensor)
    q = q + """
to_datapoints(datas).
    """
    if args.verbose:
        print(q)
    (ok, r) = user.query(q)
    assert ok == 'ok', (ok, r)
    return r


def read_AR_geo_test(args, user, sw, upper_sensor):
    q = read_resample_geo_script(args, sw, upper_sensor)
    q = q + """\n
map(def (data) ->
    am = ar_model(value(data); estimate: "ls", p: %d, const: false),
    value(ar_forecast(data; params: am, n: %d, alpha: 0.05))
end, datas).
    """ % (args.model_p, args.model_n)
    if args.verbose:
        print(q)
    (ok, r) = user.query(q)
    assert ok == 'ok', (ok, r)
    return r


def validate(args, creds):
    r = create_clients(args.test, creds)
    if r is None:
        raise ValueError("unknown test scenario: %d" % args.test)
    (user, _, attrs) = r
    r = validate1(args, user)
    if args.verbose:
        print("OK: Data are validated")
    return r


def validate1(args, user):
    data = import_csv(args, CSV)
    (ok, sws) = user.query("get_swimlanes().")
    assert ok == 'ok'
    print("Created swimlanes:")
    for sw in sws:
        print(sw)

    for sw in sws:
        (ok, res) = user.query('get_report("describe_swimlane", #{"key": "%s"}).' % sw)
        assert ok == 'ok'
        print("Describe swimlane %s:" % sw)
        #print(res)

        total, records_cb = 0, {}
        for sensor, info in res["sensors"].items():
            recs = info["total_records"]
            total += recs
            records_cb[int(sensor)] = recs
        sorted_records_cb = sorted(records_cb.items(), key=operator.itemgetter(1), reverse=True)
        print("Total: %d records" % total)
        print("Records per a sensor:")
        for sensor, recs in sorted_records_cb:
            print("%d: %d" % (sensor, recs))

        upper_sensor = res["utilized_sensors"]
        (ok, r) = user.query("""
            use("%s"),
            read $0-$%d
                select $w.alt, count($w.m), max($w.m), min($w.m), avg($w.m)
                group * by $.alt as w
                %%group * by round($.alt) as w
                format text (decimals: 2)
                %%format json (array: true)
            end.
        """ % (sw, upper_sensor))
        assert ok == 'ok', (ok, r)

        if args.verbose:
            print("Server read details:")
            print(r)
            rows1 = list(csv.reader(r.splitlines()))
            rows2 = []
            for ts, alt, cnt, mx, mn, avg in rows1:
                rows2.append((int(ts) // 1000000000, float(alt), int(cnt), float(mx), float(mn), float(avg)))
            rows2.sort(key=lambda v: v[2], reverse=True)
            for ts, alt, cnt, mx, mn, avg in rows2:
                print("ts: %d\talt: %d\tcnt: %d\tmx: %.2f\tmn: %.2f\tavg: %.2f" % (ts, int(alt), cnt, mx, mn, avg))

    return upper_sensor, sws


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
    parser.add_argument("-t", "--test", type=int, choices=range(1, 11), help="test scenario number", default=1)
    parser.add_argument('-w','--write', help='Write data', required=False, action='store_true')
    parser.add_argument('-g','--geo', help='Create geo-stationary sensors', required=False, action='store_true')
    parser.add_argument("-q", "--query", type=int, choices=range(1, 11), help="""Read scenario:
        1 - count/max/min/avg for data grouped by Alt,
        2 - apply Augmented Dickey Fuller Test to check whether the time series is non-stationary
        3 - resample
        4 - ARMA
        5 - AR
        6 - Support Vector Regression (SVR)
        7 - resample by geo box
        8 - AR for geo box
    """, required=False, default=1)
    parser.add_argument('-v','--validate', help='Validate data', required=False, action='store_true')
    parser.add_argument('-d','--delete', help='Clean data', required=False, action='store_true')
    parser.add_argument('-i','--info', help='Print info about test scenario/swimlane', required=False, action='store_true')
    parser.add_argument('--verbose', help='verbose: True or False', required=False, action='store_true', default=False)
    parser.add_argument('--creds', help="file with credential info", required=False)
    parser.add_argument('--csv', help="CSV file with timeseries to import", required=False)
    parser.add_argument('--model_p', type=int, choices=range(1, 21), help="AR model order", required=False, default=3)
    parser.add_argument('--model_q', type=int, choices=range(1, 21), help="MA model order", required=False, default=4)
    parser.add_argument('--model_n', type=int, help="forecast number", required=False, default=12)
    parser.add_argument('--resample_min_records', type=int, help="resample: min records number in a sensor", required=False, default=50)
    parser.add_argument('--resample_days', type=int, help="resample: group in the given number of days", required=False, default=30)
    parser.add_argument('--resample_use_m', help="resample: account for magnitude value (by default - only number of events)",
                        required=False, action='store_true')

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
