#!/usr/bin/python3
#
# cmd.py - mdtsdb command line tool
#

from __future__ import print_function

import argparse, os, sys, json, random, time, re

import pprint
pp = pprint.PrettyPrinter()

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import Style

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../common')))

from mdtsdb import Mdtsdb
import utils
from utils import (new_user, ConnectionError, create_clients, update_clients, open_creds, REQ_TIMEOUT)

CONFIG = 'shell.json'

#############################################################################

#shell_command_sign = "\\\\"
shell_command_sign = ""
shell_commands = {
    "help":   shell_command_sign + "help",
    "exit":   shell_command_sign + "exit",
    "source": shell_command_sign + "source",
    "data":   shell_command_sign + "data",
    "j":      shell_command_sign + "j",
    "whoami": shell_command_sign + "whoami"
}
shell_commands_help = {
    "help":   "  !help                    Print help about TimeEngine shell.",
    "exit":   "  !exit or !q              Exit from TimeEngine shell.",
    "source": "  !source 'path to script' Execute a script file.",
    "data":   "  !data 'path to json'     Export data from json file and send to TimeEngine.",
    "j":      "  !j code                  Execute code and pretty-print result as JSON.",
    "whoami": "  !whoami                  Print the name of the current User."
}

#############################################################################

def config_path(args, filename):
    if args.config != None:
        config_fn = args.config
    else:
        config_fn = os.path.join(os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__), '../data/')), filename)
    return config_fn

def read_config(args):
    try:
        config_filename = config_path(args, CONFIG)
        with open(config_filename) as json_file:
            data = json.load(json_file)
            assert data.get("key")
            assert data.get("secret_key")
            return ("ok", data)
    except Exception as e:
        return ("error", str(e))

def write_config(args, info):
    config_filename = config_path(args, CONFIG)
    with open(config_filename, 'w') as outfile:
        data = {
            "key": info["key"],
            "secret_key": info["secret_key"]
        }
        json.dump(data, outfile, indent=4)
        return ("ok", "")

def get_su(args):
    return Mdtsdb(
        host=utils.HOST,
        port=utils.PORT,
        admin_key=utils.MasterKey,
        secret_key=utils.MasterSecret,
        timeout=REQ_TIMEOUT,
        is_https=utils.ISHTTPS)

def create_user(args, su):
    if args.builtin:
        return su
    (ok, result) = read_config(args)
    if args.user_key:
        user = Mdtsdb(
            host=utils.HOST,
            port=utils.PORT,
            admin_key=args.user_key,
            secret_key=args.user_secret,
            timeout=REQ_TIMEOUT,
            is_https=utils.ISHTTPS)
        write_config(args, {"key": args.user_key, "secret_key": args.user_secret})
        return user
    elif ok == "ok":
        return Mdtsdb(
            host=utils.HOST,
            port=utils.PORT,
            admin_key=result["key"],
            secret_key=result["secret_key"],
            timeout=REQ_TIMEOUT,
            is_https=utils.ISHTTPS)
    else:
        print("fatal error: require either config file, or user credentials")
        raise(EOFError)

def run_query(args, active_user, qtext, as_json):
    (ok, result) = active_user.query(qtext)
    if as_json:
        pp.pprint(json.dumps(result, indent=4, sort_keys=True))
    else:
        pp.pprint(result)
    return True

def run_source(args, active_user, qtext):
    parts = qtext.split()
    try:
        with open(parts[1]) as script:
            run_query(args, active_user, script.read(), False)
    except FileNotFoundError as e:
        print(str(e) + ": " + parts[1])
        return False
    except IndexError:
        print("  Unexpected command format, should be:")
        print(shell_commands_help["source"])
        return False
    return True

def run_data(args, active_user, qtext):
    parts = qtext.split()
    try:
        with open(parts[1]) as json_file:
            multi_payload = json.load(json_file)
            (status, result) = active_user.insert(multi_payload)
            assert status == 'ok', (status, result)
            print(result)
    except FileNotFoundError as e:
        print(str(e) + ": " + parts[1])
        return False
    except IndexError as e:
        print("  Unexpected command format, should be:")
        print(shell_commands_help["data"])
        return False
    return True

def run_help(args, qtext):
    parts = qtext.split()
    if len(parts) == 1:
        for cmd in shell_commands:
            print(shell_commands_help[cmd])
    elif len(parts) == 2:
        print(shell_commands_help[parts[1]])
    else:
        print("  Unexpected command format, should be:")
        print(shell_commands_help["help"])
        return False
    return True

#############################################################################

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TimeEngine Python SDK shell')
    parser.add_argument('-s','--server', help='TimeEngine server host', required=False)
    parser.add_argument('-p','--port', help='TimeEngine server port', required=False)
    parser.add_argument('--use_https', help='Use https scheme', required=False, default=False, action='store_true')
    parser.add_argument('--config', help="File with config info", required=False)
    parser.add_argument('--key', help='Builtin User key', required=False)
    parser.add_argument('--secret', help='Builtin User secret', required=False)
    parser.add_argument('--user_key', help='Active User key', required=False)
    parser.add_argument('--user_secret', help='Active User secret', required=False)
    parser.add_argument('--builtin', help='Run in builtin user mode', required=False, action='store_true')
    parser.add_argument('--multiline', help='Run in multi-line mode (Enter - a new line, Meta+Enter - execute)',
                        required=False, action='store_true')

    args = parser.parse_args()

    if args.server != None:
        utils.HOST = args.server
    if args.port != None:
        utils.PORT = int(args.port)
    if args.use_https != None:
        utils.ISHTTPS = args.use_https

    if args.key != None:
        utils.MasterKey = args.key
    if args.secret != None:
        utils.MasterSecret = args.secret

    if args.multiline:
        multiline = True
        print("multi-line editing mode:\n    press Enter to insert a new line\n    Meta+Enter or Esc+Enter is to execute an input")
    else:
        multiline = False

    try:
        session = PromptSession(
#            lexer=PygmentsLexer(SqlLexer), completer=sql_completer, style=style
        )
        super_user = None
        active_user = None
        qtext = None
        while True:
            try:
                if not super_user:
                    print("connecting to %s:%s ..." % (str(utils.HOST), str(utils.PORT)))
                    # creating a super-user
                    super_user = get_su(args)
                    # creating a User
                    active_user = create_user(args, super_user)
                    print("connected as %s ..." % str(active_user.admin_key))
                qtext = session.prompt(">>> ", multiline=multiline)
                if qtext == "":
                    continue
                elif qtext == "!help" or qtext == "!h":
                    run_help(args, qtext)
                elif qtext == "!whoami" or qtext == "!w":
                    print("connected as: %s" % str(active_user.admin_key))
                elif qtext == "!source":
                    run_source(args, active_user, qtext)
                elif qtext == "!data":
                    run_data(args, active_user, qtext)
                elif qtext == "!exit" or qtext == "!q":
                    break
                elif re.match("^!j ", qtext):
                    run_query(args, active_user, qtext[3:], True)
                else:
                    run_query(args, active_user, qtext, False)
            except ConnectionError as e:
                print(repr(e))
                super_user = None
                active_user = None
            except KeyboardInterrupt:
                continue  # Control-C pressed. Try again.
            except EOFError:
                break  # Control-D pressed.
            except Exception as e:
                print(repr(e))

        print("GoodBye!")

    except Exception as e:
        print(repr(e))


#############################################################################
