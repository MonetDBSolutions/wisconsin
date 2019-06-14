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
import logging
import time
import pymonetdb
import os
import itertools
import json


class MonetDB:

    @staticmethod
    def run(task, debug=True):
        """
        :param task:
        :param debug:
        :return:
        """
        if debug:
            logging.info(f'Task:{task}')
        options = json.loads(task['options'])
        if 'runlength' in options:
            runlength = int(options['runlength'])
        else:
            runlength = 1

        before = ''
        after = ''
        if 'before' in task:
            before = task['before']
        if 'after' in task:
            after = task['after']

        response = []
        error = ''
        # always run an experiment in a cleanly started connection
        try:
            conn = pymonetdb.connect(database=task['db'])
        except (Exception, pymonetdb.DatabaseError) as msg:
            logging.error(f"EXCEPTION {msg}")
            return {'error': msg, 'times': [], 'chks': [], 'param': []}

        if task['params']:
            data = [json.loads(task['params'][k]) for k in task['params'].keys()]
            names = [d for d in task['params'].keys()]
            gen = itertools.product(*data)
        else:
            gen = [[1]]
            names = ['_ * _']

        for z in gen:
            if error != '':
                break
            if debug:
                logging.info(f"Run query: {time.strftime('%Y-%m-%d %H:%m:%S', time.localtime())}")
                logging.info(f'Parameter: {z}')
                logging.info(task['query'])

            args = {}
            for n, v in zip(names, z):
                if task['params']:
                    args.update({n: v})
            try:
                preload = [v for v in list(os.getloadavg())]
            except os.error:
                preload = 0

            times = []
            chks = []

            newquery = task['query']
            if z:
                if debug:
                    logging.info(f'args:{args}')
                # replace the variables in the query
                for elm in args.keys():
                    newquery = re.sub(elm, str(args[elm]), newquery)
                    if before:
                        before = re.sub(elm, str(args[elm]), before)
                    if after:
                        after = re.sub(elm, str(args[elm]), after)
                if debug:
                    logging.info(f'New query {newquery}')
                    if before:
                        logging.info(f'Before {before}')
                    if after:
                        logging.info(f'Before {after}')

            for i in range(runlength):
                try:
                    c = conn.cursor()
                    if before:
                        c.execute(before)
                    ticks = time.time()
                    c.execute(newquery)
                    r = c.fetchone()
                    if r:
                        chks.append(int(r[0]))
                    else:
                        chks.append('')
                    times.append(int((time.time() - ticks) * 1000))
                    if after:
                        c.execute(after)

                    if debug:
                        print('ticks[%s]' % i, times[-1])
                    c.close()
                except (Exception, pymonetdb.DatabaseError) as msg:
                    logging.error(f'EXCEPTION  {msg}')
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
            conn.close()
        return response
