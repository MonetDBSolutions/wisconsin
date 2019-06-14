"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M Kersten
The code to contact the Sqalpel webserver is organized here.
"""
import requests
import json
import os
import logging


class Sqalpel:
    # keep just enough information to contact the web server
    # and some session based
    server = 'localhost:5000'
    ticket = None
    timeout = None
    debug = True
    memory = 0

    def __init__(self, args):
        """
        Contact the sql server and gather some basic information of the platform to identify the platform results.
        :param config:
        :return:
        """

        self.server = args.server
        self.ticket = args.ticket
        self.timeout = args.timeout
        self.debug = args.debug
        try:
            mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
            self.memory = mem_bytes / (1024. ** 3)
        except ValueError:
            pass

    def get_work(self, debug=False):
        endpoint = 'http://' + self.server + '/get_work'
        args = {'ticket': self.ticket}
        if self.debug:
            logging.info(f'Ticket used {self.ticket}')

        response = ''
        try:
            if debug:
                logging.info(f'Endpoint {endpoint}')
                logging.info(f'Requesting {json.dumps(args, sort_keys=True, indent=4)}')
            response = requests.get(endpoint,  json=args, timeout=20)
        except requests.exceptions.RequestException as e:
            logging.error(f'REQUESTS exception {e}')
            return None

        if debug:
            logging.info(f'WEB SERVER RESPONSE {response}')
        if response.status_code != 200:
            return None

        task = json.loads(response.content)
        if not task:
            logging.info(f'No tasks available for the target section {json.dumps(args, sort_keys=True, indent=4)}')

        if 'extras' in task:
            logging.info(f"extras {task['extras']}")
            e = json.loads(task['extras'])
            task.update(e)

        if self.debug:
            logging.info(f'Task received: {json.dumps(task, sort_keys=True, indent=4)}')
        return task

    def put_work(self, task, results):
        if results is None:
            if self.debug:
                logging.info('Missing result object')
            return None

        endpoint = 'http://' + self.server + '/put_work'
        # prepare the answer for the webserver
        u = { 'ticket': task['ticket'],
              'db': task['db'],
              'dbms': task['dbms'],
              'host': task['host'],
              'project': task['project'],
              'experiment': task['experiment'],
              'tag': task['tag'],
             }
        u.update({'runs': results})

        # move the message to the webserver
        response = ''
        try:
            if self.debug:
                logging.info(f'sending {json.dumps(u, sort_keys=True, indent=4)}')
            response = requests.post(endpoint, json=u)
            if self.debug:
                logging.info(f'Sent task result{response}')
            return response.status_code == 200
        except requests.exceptions.RequestException as e:
            logging.error(f'REQUESTS exception {e}')
            logging.error(f'Failed to post to {endpoint}')
        logging.info(f'Sent task result{response}')
        return False
