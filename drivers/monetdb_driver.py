"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M Kersten

Execute a single query multiple times on the database nicknamed 'db'
and return a list of timings. The first error encountered aborts the sequence.
The result is a list of dictionaries
run: [{
    times: [<response time>]
    chks: [<integer value to represent result (e.g. cnt,  checksum or hash over result set) >]
    param: {param1:value1, ....}
    errors: []
    }]

If parameter value lists are given, we run the query for each element in the product.

Internal metrics, e.g. cpu load, is returned as a JSON structure in 'metrics' column
"""

import re
import subprocess
import shlex
import time
import pymonetdb
import os
import itertools
import json

def runtask(task):
    logging.info(f'run task {task}')
    if task['dbms'].lower() == 'monetdb':
        results = MonetDBDriver.run(task)
    else:
        results = None
        print('Undefined task platform', task['dbms'])
    return results

def runbatch(queries):
    logging.info('Run a batch of queries')
    results = []
    for q in queries:
        task = {'dbms': args.dbms,
                'query':q['query'],
                'xname': q['xname'],
                'db': args.db,
                'params': '',
                'options': '{}',
                'debug': args.debug}
        r = runtask(task)
        results.append(r)
    return results

class MonetDBDriver:
    conn = None
    db = None

    def __init__(self):
        pass

    @staticmethod
    def startserver(db):
        if MonetDBDriver.conn:
            # avoid duplicate connection
            if MonetDBDriver.db == db:
                return None
            MonetDBDriver.stopserver()
        print('Start MonetDBDriver', db)
        try:
            MonetDBDriver.conn = pymonetdb.connect(database=db)
        except (Exception, pymonetdb.DatabaseError()) as msg:
            print('EXCEPTION ', msg)
            if MonetDBDriver.conn is not None:
                MonetDBDriver.close()
            return [{'error': json.dumps(msg), 'times': [], 'chks': [], 'param': []}]
        return None

    @staticmethod
    def stopserver():
        if not MonetDBDriver.conn:
            return None
        print('Stop MonetDBDriver')
        # to be implemented
        return None

    @staticmethod
    def run(task):
        """
        :param task:
        :return:
        """
        debug = task['debug']
        db = task['db']
        query = task['query']
        params = task['params']
        options = json.loads(task['options'])
        if 'runlength' in options:
            runlength = int(options['runlength'])
        else:
            runlength = 1
        print('runs', runlength)

        response = []
        error = ''
        msg = MonetDBDriver.startserver(db)
        if msg:
            MonetDBDriver.stopserver()
            return msg

        if params:
            data = [json.loads(params[k]) for k in params.keys()]
            names = [d for d in params.keys()]
            gen = itertools.product(*data)
        else:
            gen = [[1]]
            names = ['_ * _']

        for z in gen:
            if error != '':
                break
            if debug:
                print('Run query:', time.strftime('%Y-%m-%d %H:%m:%S', time.localtime()))
                print('Parameter:', z)
                print(query)

            args = {}
            for n, v in zip(names, z):
                if params:
                    args.update({n: v})
            try:
                preload = [ v for v in list(os.getloadavg())]
            except os.error:
                preload = 0

            times = []
            chks = []
            newquery = query
            if z:
                if debug:
                    print('args:', args)
                # replace the variables in the query
                for elm in args.keys():
                    newquery = re.sub(elm, str(args[elm]), newquery)
                print('New query', newquery)

            for i in range(runlength):
                try:
                    c = MonetDBDriver.conn.cursor()

                    ticks = time.time()
                    c.execute(newquery)
                    r = c.fetchone()
                    if r:
                        chks.append(int(r[0]))
                    else:
                        chks.append('')
                    times.append(int((time.time() - ticks) * 1000))

                    if debug:
                        print('ticks[%s]' % i, times[-1])
                    c.close()
                except (Exception, pymonetdb.DatabaseError) as msg:
                    print('EXCEPTION ', msg)
                    error = str(msg).replace("\n", " ").replace("'", "''")
                    break

            # wrapup the experimental runs,
            # The load can be sent as something extra, it is an internal metric
            try:
                postload = [v for v in list(os.getloadavg())]
            except os.error:
                postload = 0

            res = {'times': times,
                   'chksum': chks,
                   'param': args,
                   'error': error,
                   'metrics': {'load': preload + postload},
                   }

            response.append(res)
        if debug:
            print('Finished the run')
        MonetDBDriver.stopserver()
        return response
