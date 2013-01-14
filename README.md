logressor
=========

python script for convert log files to sqlite based on regexp named groups

todo
====
- add possibility to load regexp and formatting from config file
- multiline log processing
- documentation :)

usage
=====
```
> python logressor.py -h
logressor.py 0.2
usage: logressor.py [-h] [--format FORMAT] [--table [TABLE]] [-v] [-d]
                    [--drop]
                    FILE REGEXP SQLITEFILE

This script is able to convert log files to sqlite format based 
on regexp named group method.

positional arguments:
  FILE             log file to work on
  REGEXP           regexp with named groups to separate log values
  SQLITEFILE       the result sqlite file name

optional arguments:
  -h, --help       show this help message and exit
  --format FORMAT  format of named groups in parseable dict
  --table [TABLE]  the table name in sqlite database
  -v, --version
  -d, --debug      debug (default: False)
  --drop           drop table before create (default: False)

Sample usage:
  python logressor.py \
    sample/s.log \
    "^(?P<v1>.{15})\s+(?P<v2>\S+)\s+(?P<v3>\S+)*" \
    sample/output.sqlite \
    --format "{'v1':{'type':'timestamp','format':'%b %d %H:%M:%S'},'v2':{'type':'real'}}" 
```