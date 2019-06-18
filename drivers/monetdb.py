"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M Kersten

The prototypical MonetDB driver to run a Sqalpel experitment and report on it.
"""

import logging
import pymonetdb
import os


class MonetDB:

    @staticmethod
    def run(sqalpel):
        """
        :param sqalpel:
        :return:
        """
        logging.info(f"Run {sqalpel.task}")

        # Establish a clean connection
        try:
            conn = pymonetdb.connect(database=sqalpel.db)
        except (Exception, pymonetdb.DatabaseError) as msg:
            sqalpel.error = msg
            logging.error(f"EXCEPTION {msg}")
            return

        # Collects all variants of an experiment
        for before, query, after in sqalpel.generate():
            if sqalpel.debug:
                logging.info(f"EXECUTE BEFORE {before}")
                logging.info(f"EXECUTE QUERY  {query}")
                logging.info(f"EXECUTE AFTER  {after}")

            # Process all experiments multiple times
            for i in range(sqalpel.runlength):
                try:
                    # Collect some system metrics
                    preload = [v for v in list(os.getloadavg())]
                except os.error:
                    preload = 0
                sqalpel.metrics = {'load': preload}
                try:
                    c = conn.cursor()
                    if before:
                        c.execute(before)

                    sqalpel.start()
                    c.execute(query)
                    try:
                        # if we have a result set, then obtain first row to represent it
                        r = c.fetchone()
                        if r:
                            sqalpel.keep(r)
                        else:
                            sqalpel.keep('')
                    except (Exception, pymonetdb.DatabaseError) as e:
                        sqalpel.error = e
                    sqalpel.done()

                    if after:
                        c.execute(after)

                    c.close()
                except (Exception, pymonetdb.DatabaseError) as msg:
                    logging.error(f'EXCEPTION  {msg}')
                    sqalpel.error = str(msg).replace("\n", " ").replace("'", "''")
                    break

        # Establish a clean connection
        try:
            conn.close()
        except (Exception, pymonetdb.DatabaseError) as msg:
            sqalpel.error = msg
            logging.error(f"EXCEPTION {msg}")
            return
