### Setting up the database
The Github repository supports the creation of two databases:
basic and large. The former is the original small one composed of
the tables ONEKTUP and TEN10KTUP{12}.
The latter is the extended version described in [1] with tables
HUNDREDKTUP{1,2}

The shell script in data/setup.sh illustrates how it is
instantiated for MonetDB. Other target database may require
minor adjustments.

The database variants available should be documented in the
sqalpel.yaml file. Likewise, the specific scripts to load
the data in a particular dbms