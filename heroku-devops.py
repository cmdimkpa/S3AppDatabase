from __future__ import division
import subprocess
import redis
import datetime
import json
import sys
import requests as http
from time import sleep

args = sys.argv
gateway = args[2]

def run_shell(cmd):
  p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, shell=True)
  out, err = p.communicate()
  if err:
      return err
  else:
      try:
          return eval(out)
      except:
          return out


def clean(x):
    x = x.replace(' ', '')
    x = x.replace('\n', '')
    return x


def log_entry(message):
    payload = {
        "tablename": "redis_flush_entries",
        "data": {
            "message": message,
            "when": str(datetime.datetime.today())
        }
    }
    http.post("https://%s.herokuapp.com/ods/new_record" % gateway,
              json.dumps(payload), headers={"Content-Type": "application/json"})
    return None


try:

    mode = args[1].lower()

    if mode == "manage-cache":
        # parameters
        try:
            duration_seconds = int(args[3])
            snapshot_interval = int(args[4])
        except:
            sys.exit()
        # login prior if necessary (`heroku login`)
        # parse REDIS CONFIG
        config = run_shell('heroku config -a %s | grep REDIS' % gateway)
        config = config.replace("redis://h:", "")
        config = config.replace("@", ":")
        password, host, port = map(lambda x: clean(x), config.split(":"))[1:]

        # create connection to remote redis instance
        pool = redis.ConnectionPool(
            password=password, host=host, port=port, db=0)
        r = redis.Redis(connection_pool=pool)

        # trigger flushdb on dyno idle status
        if 'idle' in run_shell('heroku ps -a %s' % gateway):
            # flush db
            k = len(r.keys())
            r.flushdb()
            # log entry
            log_entry("TOTAL flush executed: %s keys deleted" % k)
        else:
            # flush looped snapshot
            snapshots_required = int(duration_seconds/snapshot_interval)
            total_deleted = 0
            for i in xrange(snapshots_required):
                # take snapshot
                snapshot = r.keys()
                # allow to mature
                sleep(snapshot_interval)
                # delete matured snapshot
                map(lambda key: r.delete(key), snapshot)
                total_deleted += len(snapshot)
            # log entry
            log_entry("SNAPSHOT flush executed: %s keys deleted" %
                      total_deleted)

    if mode == "try-restart":
        # check for `crashed` in app state
        if "crashed" in run_shell('heroku ps -a %s' % gateway).lower():
            # restart app
            run_shell('heroku ps:restart -a %s' % gateway)

except:
    sys.exit()
