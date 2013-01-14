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
  python logressor.py sample/s.log "^(?P<v1>.{15})\s+(?P<v2>\S+)\s+(?P<v3>\S+)*" "{'v1':{'type':'timestamp','format':'%b %d %H:%M:%S'},'v2':{'type':'real'},'v3':'text'}" sample/output.sqlite
