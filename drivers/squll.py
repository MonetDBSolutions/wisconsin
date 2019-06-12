"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel.

Author: M Kersten
This program is intended run the experiments against MonetDB.
It either contacts the sqapel website or uses a experiments.yaml file.

Accessing the website requires a session key obtained using a session
login to the website.
"""

import argparse
import time
import yaml
import logging

from repository import Repository
from connection import Connection
from monetdb_driver import runtask, runbatch

parser = argparse.ArgumentParser(
    description='This program is the MonetDB experiment driver for SQALPEL.io. '
                'It requires an account on SQALPEL.io and being a member of the project team. '
                'To contribute results you have to obtain a task ticket.'
                'With the task ticket you can obtain the details of a single task.'
                ''
                'The program should be started on each machine on which you want to perform an experiment. '
                'Offline experimentation against the baseline is supported as well.',
    formatter_class=argparse.HelpFormatter)


parser.add_argument('--ticket', type=str, help='Ticket', default='local')
parser.add_argument('--driver', type=str, help='Target driver', default='MonetDB')
parser.add_argument('--repository', type=str, help='Project Git', default='https://github.com/sqalpel/wisconsin.git')
parser.add_argument('--dbms', type=str, help='Default DBMS', default='MonetDB')
parser.add_argument('--db', type=str, help='Default database', default='base')
parser.add_argument('--host', type=str, help='Default host', default=None)
parser.add_argument('--debug', help='Trace interaction', action='store_false')
parser.add_argument('--version', help='Show version info', action='store_true')


if __name__ == '__main__':
    args = parser.parse_args()
    if args.version:
        print('squll version 0.5')

    # The log information is gathered in a single file
    LOGFILE = time.strftime("%H:%M", time.localtime())
    logging.basicConfig(level=logging.INFO,
                        # filename= f'logs/{LOGFILE}',
                        # filemode='w',
                        format='%(levelname)-7s %(asctime)s  %(message)s',
                        datefmt='%H:%M:%S')

    # Parse the sqalpel configuration
    if not args.repository or not Repository.isvalid(args.repository):
        print(f'Invalid repository {args.repository}')
        exit(-1)

    repos = Repository.get_yaml(args.repository, 'sqalpel.yaml')
    logging.info(repos)
    if not repos:
        print('Error in sqalpel.yaml')
        exit(-1)
    elif 'parse_error' in repos:
        print(f"Parse error in sqalpel.yaml {args.repository}")
        exit(-1)

    if 'driver' not in repos:
        print(f'Unkown drivers')
        print('Known drivers ', [n for n in repos['driver']])
        exit(0)

    # sanity check on the configuration file
    driver = repos.dbms[args.dbms]
    if args.debug:
        logging.info(f"DRIVER {driver}")

    # check local/remote processing
    if args.ticket == 'local':
        if not args.repository:
            print(f'Missing repository URL')
            exit(-1)
        if not args.db:
            print(f'Missing database name')
            exit(-1)
        if not args.dbms:
            print(f'Missing DBMS name')
            exit(-1)
        if not args.host:
            print(f'Missing host')
            exit(-1)

    # process the queries in the repository
    queries = None
    if args.repository:
        if args.debug:
            print(f'process Git repository {args.repository}')
        if Repository.isvalid(args.repository):
            errors, queries = Repository.get_experiments(args.repository)
            if args.debug:
                print(f'ERRORS: {errors}')
                print(f'QUERIES: {queries}')
            results = runbatch(queries)
            print(results)
            exit(0)
        else:
            print(f'Invalid repository URL {args.repository}')

    # Connect to the sqalpel.io webserver for the real work
    conn = Connection(section)

    delay = 5
    bailout = section['bailout']

    while True:
        task = conn.get_work(section)
        if task is None:
            print('Lost connection with sqalpel.io server')
            break

        # If we don't get any work we either should stop or wait for it
        if 'error' in task:
            print('Server reported an error:', task)
            if task['error'] == 'Out of work' or task['error'] == 'Unknown task ticket':
                if not section['daemon']:
                    break
                print('Wait %d seconds for more work' % delay)
                time.sleep(delay)
                if delay < 60:
                    delay += 5
                else:
                    break
                continue
            elif task['error']:
                bailout -= 1
                if bailout == 0:
                    print('Bail out after too many database server errors')
                    break
            continue

        if args.get:
            print(task)
            exit(0)

        results = runtask(task)

        print(results)
        if results and results[-1]['error'] != '':
            bailout -= 1
            if bailout == 0:
                print('Bail out after too many database errors')
                break
        if not conn.put_work(task, results):
            print('Error encountered in sending result')
            if not section['daemon']:
                break

    if args.debug:
        print('Finished all the work')
