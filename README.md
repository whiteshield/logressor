logressor
=========

python script for convert log files to sqlite based on regexp named groups

todo
====
- multiline log processing
- documentation :)

usage
=====
```
> python logressor.py -h
logressor.py 0.3
usage: logressor.py [-h] [--file FILE] [--regexp REGEXP] [--format FORMAT]
                    [--logtype TYPE] [--list TYPE] --sqlite SQLITEFILE
                    [--table [TABLE]] [-v] [-d] [--drop]

This script is able to convert log files to sqlite format based 
on regexp named group method.

optional arguments:
  -h, --help           show this help message and exit
  --file FILE          log file to work on (or standard input, if parameter
                       not given)
  --regexp REGEXP      regexp with named groups to separate log values
  --format FORMAT      format of named groups in parseable dict
  --logtype TYPE       predefined log type from logressor.dict or user.dict
  --list TYPE          list predefined log types from logressor.dict or
                       user.dict
  --sqlite SQLITEFILE  the result sqlite file name (or standard output, if
                       parameter not given)
  --table [TABLE]      the table name in sqlite database
  -v, --version
  -d, --debug          debug (default: False)
  --drop               drop table before create (default: False)

Sample usage:
 Process sample 1)
  python logressor.py \
    --file sample/s.log \
    --regexp "^(?P<v1>.{15})\s+(?P<v2>\S+)\s+(?P<v3>\S+)*" \
    --sqlite sample/output.sqlite \
    --format "{'v1':{'type':'timestamp','format':'%b %d %H:%M:%S'},'v2':'real'}" \
    --drop

 Process sample 2) (copy user.dict-sample to user.dict)
  python logressor.py --file sample/s.log --logtype sample --sqlite sample/output.sqlite --drop

 Process appache access.log
  cat sample/access.log | python logressor.py --logtype apache --sqlite sample/output.sqlite --drop

```