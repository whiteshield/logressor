#!/usr/bin/env python
# -*- coding: utf-8 -*-
# logressor
import datetime
import re
import os
import sqlite3
import ast
import argparse

class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value



class logressor:
    """
    Log manipulator class
    """
    def __init__(self):
        """Variable initialization."""
        self.debug=False
        self.file=''
        self.regexp=''
        self.format=''
        self.sqlite=''
        self.table='data'
        self.drop=False
        self.sql=''
        self.fields=[]
        self.timestamps={}

    def setInputDebug(self, debug):
        """Set the class variable debug."""
        self.debug=debug
    def setInputDrop(self, drop):
        """Set the class variable drop."""
        self.drop=drop
    def setInputFile(self, file):
        """Set the class variable file."""
        self.file=file
    def setInputRegexp(self, regexp):
        """Set the class variable regexp."""
        self.regexp=regexp
    def setInputFormat(self, format):
        """Set the class variable format."""
        self.format=format
    def setInputSqlite(self, sqlite):
        """Set the class variable sqlite."""
        self.sqlite=sqlite
    def setInputTable(self, table):
        """Set the class variable table."""
        if table != None:
            self.table=table
    def showOptions(self):
        """Display configuration options."""
        print ""
        print "     debug: " + str(self.debug)
        print "      file: " + str(self.file)
        print "    regexp: " + str(self.regexp)
        print "    format: " + str(self.format)
        print "    sqlite: " + str(self.sqlite)
        print "     table: " + str(self.table)
        print "      drop: " + str(self.drop)
        print "    fields: " + str(self.fields)
        print "timestamps: " + str(self.timestamps)
        print "       sql: " + str(self.sql)
        print ""
    def process(self):
        """Start the program!"""
        finalSql="\n"
        if self.drop:
            finalSql = finalSql + "drop table if exists " + self.table + ";\n"
        finalSql = finalSql + "create table if not exists " + self.table +" \n" + self.sql
        self.sql = finalSql
        if self.debug:
            self.showOptions()

        try:
            self.connection = sqlite3.connect(self.sqlite)
            cursor = self.connection.cursor()    
            #insert into d values (null, '11:10', 'front', 'method', 'POST', 20, 20);
            sqlCommands=self.sql.split(';')
            for query in sqlCommands:
                cursor.execute(query)
            self.parseFile()
        except sqlite3.Error, e:
            print "Sqlite error %s:" % e.args[0]
            sys.exit(1)
        finally:
            #self.testFormat(self.format)
            if self.connection:
                self.connection.close()

    def testFormat(self, format):
        self.sql='(\n'
        try:
            success=True
            formatDict=ast.literal_eval(format)
            for fieldName,attributes in formatDict.iteritems():
                fieldType=''
                fieldFormat=''
                if type(attributes) is dict:
                    if attributes.get('type'):
                        fieldType=attributes.get('type')
                    else:
                        success=False
                    if attributes.get('format') and fieldType == 'timestamp':
                        fieldFormat=attributes.get('format')
                        self.timestamps[fieldName]=fieldFormat
                        fieldType = 'text'
                elif type(attributes) is str:
                    fieldType=attributes
                else:
                    success=False
                if not fieldType in ('real', 'text', 'integer'):
                    success=false
                self.fields.append(fieldName)
                self.sql = self.sql + "  " + fieldName + " " + fieldType + ", \n"
            self.sql=self.sql[0:-3] + '\n)\n'
            return success
        except Exception, e:
            return False

    def parseFile(self):
        cursor = self.connection.cursor()
        regex1 = re.compile(self.regexp)
        result = AutoVivification()
        i=0
        for line in open(self.file):
            parse = line.strip()
            if len(parse)>0:
                r1 = regex1.search(parse)
                result_set = {}
                if r1:
                    resultDict=r1.groupdict()
                    fieldList=[]
                    valueList=[]
                    for f in self.fields:
                        fieldList.append(f)
                        if resultDict.get(f):
                            if f in self.timestamps:
                                value = datetime.datetime.strptime(resultDict.get(f), self.timestamps.get(f))
                                if value.year == 1900:
                                    formattedDate=value.strftime(str(datetime.datetime.now().year) + '-%m-%d %H:%M:%S')
                                else :
                                    formattedDate=value.strftime('%Y-%m-%d %H:%M:%S')
                                valueList.append(formattedDate)
                            else:
                                valueList.append(resultDict.get(f))
                        else:
                            valueList.append(None)
                    insert="insert into "+self.table+" ("+(",".join(str(e) for e in fieldList))+") values ("+",".join("?" for i in range(0, len(fieldList)))+")"
                    try:
                        cursor.execute(insert, valueList)
                    except Exception, e:
                        print "Sqlite error %s:" % e.args[0]

                else:
                    print "Error in process: ",line,
        self.connection.commit()


def main():
    """Main. :)"""
    #class myParser(optparse.OptionParser):
    #    def format_epilog(self, formatter):
    #        return self.epilog

    usage = "usage: %prog [options]"
    description = """This script is able to convert log files to sqlite format based 
on regexp named group method."""
    epilog = """
Sample usage:
  python logressor.py \\
    sample/s.log \\
    \"^(?P<v1>.{15})\s+(?P<v2>\S+)\s+(?P<v3>\S+)*\" \\
    \"{'v1':{'type':'timestamp','format':'%b %d %H:%M:%S'},'v2':{'type':'real'},'v3':'text'}\" \\
    sample/output.sqlite

"""
    parser = argparse.ArgumentParser(description=description, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter,)
    parser.add_argument("file",
        metavar="FILE", action="store", 
        help="log file to work on")
    parser.add_argument("regexp",
        metavar="REGEXP", action="store",
        help="regexp with named groups to separate log values")
    parser.add_argument("format",
        metavar="FORMAT", action="store", 
        help="format of named groups in parseable dict")
    parser.add_argument("sqlite",
        metavar="SQLITEFILE", action="store",
        help="the result sqlite file name")
    parser.add_argument("--table",
        metavar="TABLE", action="store", dest="table", nargs="?",
        help="the table name in sqlite database")
    parser.add_argument("-v", "--version", action="version", version="%(prog)s 0.1")
    parser.add_argument("-d", "--debug",
        action="store_true", dest="debug", default=False, 
        help='debug (default: %(default)s)')
    parser.add_argument("--drop",
        action="store_true", dest="drop", default=False, 
        help='drop table before create (default: %(default)s)')
    options = parser.parse_args()
    if options.file == None or options.regexp == None or options.format == None or options.sqlite == None:
        parser.error("Incorrect number of arguments! Check required fields!")
        sys.exit()
    if not os.path.isfile(options.file):
        parser.error(options.file + " does not exists!")
        sys.exit()
    if not os.access(options.file, os.R_OK):
        parser.error(options.file + " access denied!")
        sys.exit()
    if os.path.isfile(options.sqlite) and not os.access(options.sqlite, os.W_OK):
        parser.error(options.sqlite + " access denied!")
        sys.exit()
    try:
        re.compile(options.regexp)
    except re.error, e:
        parser.error("REGEXP not valid: " + str(e))
        sys.exit()

    processor = logressor()

    if not  processor.testFormat(options.format):
        parser.error("FORMAT can't evaluate to dict!")
        sys.exit()

    processor.setInputDebug(options.debug)
    processor.setInputFile(options.file)
    processor.setInputRegexp(options.regexp)
    processor.setInputFormat(options.format)
    processor.setInputSqlite(options.sqlite)
    processor.setInputTable(options.table)
    processor.setInputDrop(options.drop)
    processor.process()

if __name__ == "__main__":
    main()




