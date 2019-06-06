"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0.  If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.

Copyright 2019- Stichting Sqalpel

Author: M Kersten
Generate the components for the Wisconsin Benchmark
"""
import random

tabletemplate = "DROP TABLE  IF EXISTS %s;\n" \
                "CREATE TABLE %s(\n" \
                "unique1 integer NOT NULL,\n" \
                "unique2 integer NOT NULL,\n" \
                "two integer NOT NULL,\n" \
                "four integer NOT NULL,\n" \
                "ten integer NOT NULL,\n" \
                "twenty integer NOT NULL,\n" \
                "hundred integer NOT NULL,\n" \
                "thousand integer NOT NULL,\n" \
                "twothous integer NOT NULL,\n" \
                "fivethous integer NOT NULL,\n" \
                "tenthous integer NOT NULL,\n" \
                "odd100 integer NOT NULL,\n" \
                "even100 integer NOT NULL,\n" \
                "stringu1 varchar(52) NOT NULL,\n" \
                "stringu2 varchar(52) NOT NULL,\n" \
                "string4 varchar(52) NOT NULL,\n" \
                "PRIMARY KEY (unique2)\n" \
                ");\n"

def gen_schema(name):
    return tabletemplate % (name, name)

def gen_load(name, cnt):
    return f"copy {cnt} records into {name} from '@PWD/data/{name}.csv';\n"

patterns = [
    "AxxxxxxxxxxxxxxxxxxxxxxxxAxxxxxxxxxxxxxxxxxxxxxxxA",
    "HxxxxxxxxxxxxxxxxxxxxxxxxHxxxxxxxxxxxxxxxxxxxxxxxH",
    "OxxxxxxxxxxxxxxxxxxxxxxxxOxxxxxxxxxxxxxxxxxxxxxxxO",
    "VxxxxxxxxxxxxxxxxxxxxxxxxVxxxxxxxxxxxxxxxxxxxxxxxA"
    ]
variant= [ 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'V']

def gen_table(name, cnt):
    u1 = [ i for i in range(0,cnt)]
    u2 = [ i for i in range(0,cnt)]
    random.seed(0)

    with open(f"{name}.csv", 'w') as f:
        for c in range(0,cnt):
            i1 = random.randint(0, cnt - c - 1)
            i2 = random.randint(0, cnt - c - 1)
            unique1 = u1[i1]
            unique2 = u2[i2]
            del u1[i1]
            del u2[i2]

            s1 = variant[int(unique1 % len(variant))] + 'x' * 25 + \
                 variant[int(unique1 / len(variant)) % len(variant)] + 'x' * 24 + \
                 variant[int(unique1 / len(variant) / len(variant)) % len(variant)]
            s2 = variant[int(unique2 % len(variant))] + 'x' * 25 + \
                 variant[int(unique2 / len(variant) % len(variant))] + 'x' * 24 + \
                 variant[int(unique2 / len(variant) / len(variant)) % len(variant)]
            even = 2 + (c % 50) * 2
            odd = 1 + (c % 50) * 2
            f.write(f"{unique1},{c},{c % 2},{c % 4},{c % 10},{c % 20},{c % 100},"
                  f"{c % 1000},{c % 2000},{c % 5000},{c % 10000},"
                  f"{odd},{even},{s1},{s2}\n")
    f.close()
