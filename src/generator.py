"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M Kersten
This simple program generates the schema.sql, load.sql, and <name>.csv
files to initialize a Wisconsin Benchmark database.
Variations are easily constructed to create files acceptable for
a specific DBMS.
"""
import argparse
import table
import random

parser = argparse.ArgumentParser(
    description='wisconsin.py is the generator for the Wisonsin benchmark database scripts.',
    formatter_class=argparse.HelpFormatter)

parser.add_argument('--name', type=str, help='Table name', default='TENTUP1')
parser.add_argument('--size', type=str, help='Table size', default='10')
parser.add_argument('--version', help='Show version info', action='store_true')


if __name__ == '__main__':
    args = parser.parse_args()
    if args.version:
        print('wisconsin.py version 0.1')
    l = args.name.split(',')
    s = args.size.split(',')

    if len(l) != len(s):
        print('Incompatible name and size list arguments')
        exit()
        
    for name, size in zip(l,s):
        size = int(size)
        random.seed(size)

        with open(f"{name}schema.sql", 'w') as f:
            with open(f"{name}load.sql", 'w') as l:
                l.write(table.gen_load(name, size))
                f.write(table.gen_schema(name))
                table.gen_table(name, size)
        f.close()
        l.close()