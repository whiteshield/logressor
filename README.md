logressor
=========

python script for convert log files to sqlite based on regexp named groups

todo
====
- make same field order in sqlite like in log
- add format generation for simple logs
- add informative messages for debugging
- add possibility to load regexp and formatting from config file
- multiline log processing
- documentation :)

usage
=====
```
> python logressor.py -h
usage: logressor.py [-h] [--table [TABLE]] [-v] [-d] [--drop]
                    FILE REGEXP FORMAT SQLITEFILE

This script is able to convert log files to sqlite format based 
on regexp named group method.

positional arguments:
  FILE             log file to work on
  REGEXP           regexp with named groups to separate log values
  FORMAT           format of named groups in parseable dict
  SQLITEFILE       the result sqlite file name

optional arguments:
  -h, --help       show this help message and exit
  --table [TABLE]  the table name in sqlite database
  -v, --version
  -d, --debug      debug (default: False)
  --drop           drop table before create (default: False)

Sample usage:
  python logressor.py \
    sample/s.log \
    "^(?P<v1>.{15})\s+(?P<v2>\S+)\s+(?P<v3>\S+)*" \
    "{'v1':{'type':'timestamp','format':'%b %d %H:%M:%S'},'v2':{'type':'real'},'v3':'text'}" \
    sample/output.sqlite
```