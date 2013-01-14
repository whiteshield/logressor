#!/usr/bin/env python
# -*- coding: utf-8 -*-
# logressor
import datetime
import re
import os
import sqlite3
import ast
import argparse
import sys


class logressor:
    """
    Log manipulator class
    """
    def __init__(self):
        """Variable initialization."""
        self.debug=False
        self.file=''
        self.regexp=''
        self.format={}
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
        if format != None:
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
        self.loadFields()
        finalSql="\n"
        if self.drop:
            finalSql = finalSql + "drop table if exists " + self.table + ";\n"
        finalSql = finalSql + "create table if not exists " + self.table +" \n" + self.sql
        self.sql = finalSql

        if self.debug:
            self.showOptions()
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" process start"

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
            sys.exit()
        finally:
            if self.connection:
                self.connection.close()
        if self.debug:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" process end"
        print self.file + " loaded into " + self.sqlite

    def loadFields(self):
        regexp1=re.compile("\?P<(\w+)>")
        results=regexp1.findall(self.regexp) 
        if results:
            self.fields=results
        self.sql='(\n'
        try:
            formatDict=ast.literal_eval(self.format)
        except:
            formatDict={}
        for fieldName in self.fields:
            fieldType=''
            fieldFormat=''
            if formatDict.get(fieldName):
                if type(formatDict.get(fieldName)) is dict:
                    if formatDict.get(fieldName).get('type'):
                        fieldType=formatDict.get(fieldName).get('type')
                    else:
                        fieldType='text'
                    if formatDict.get(fieldName).get('format') and fieldType == 'timestamp':
                        fieldFormat=formatDict.get(fieldName).get('format')
                        self.timestamps[fieldName]=fieldFormat
                        fieldType = 'text'
                elif type(formatDict.get(fieldName)) is str:
                    fieldType=formatDict.get(fieldName)
                else:
                    fieldType='text'
                if not fieldType in ('real', 'text', 'integer'):
                    fieldType='text'
            else:
                fieldType='text'
            self.sql = self.sql + "  " + fieldName + " " + fieldType + ", \n"
        self.sql=self.sql[0:-3] + '\n)\n'



    def testFormat(self, format):
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
                        fieldType = 'text'
                elif type(attributes) is str:
                    fieldType=attributes
                else:
                    success=False
                if not fieldType in ('real', 'text', 'integer'):
                    success=False
            return success
        except Exception, e:
            return False

    def parseFile(self):
        if self.debug:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" parse start"
        linesOK=0;
        linesFail=0
        cursor = self.connection.cursor()
        regex1 = re.compile(self.regexp)
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
                        linesOK+=1
                    except Exception, e:
                        print "Sqlite error %s:" % e.args[0]
                        linesFail+=1

                else:
                    print "Error in process: ",line,
                    linesFail+=1
        if self.debug:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" parse end with total "+str(linesOK+linesFail)+" lines (OK: "+str(linesOK)+"/ Fail: "+str(linesFail)+")"
        if self.debug:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" commit start"
        self.connection.commit()
        if self.debug:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" commit end"


def main():
    """Main. :)"""
    #class myParser(optparse.OptionParser):
    #    def format_epilog(self, formatter):
    #        return self.epilog

    prog="logressor.py"
    version=0.2
    print prog,str(version)
    description = """This script is able to convert log files to sqlite format based 
on regexp named group method."""
    epilog = """
Sample usage:
  python logressor.py \\
    sample/s.log \\
    \"^(?P<v1>.{15})\s+(?P<v2>\S+)\s+(?P<v3>\S+)*\" \\
    sample/output.sqlite \\
    --format \"{'v1':{'type':'timestamp','format':'%b %d %H:%M:%S'},'v2':{'type':'real'}}\" 

"""
    parser = argparse.ArgumentParser(description=description, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter, prog=prog)
    parser.add_argument("file",
        metavar="FILE", action="store", 
        help="log file to work on")
    parser.add_argument("regexp",
        metavar="REGEXP", action="store",
        help="regexp with named groups to separate log values")
    parser.add_argument("--format",
        metavar="FORMAT", action="store", 
        help="format of named groups in parseable dict")
    parser.add_argument("sqlite",
        metavar="SQLITEFILE", action="store",
        help="the result sqlite file name")
    parser.add_argument("--table",
        metavar="TABLE", action="store", dest="table", nargs="?",
        help="the table name in sqlite database")
    parser.add_argument("-v", "--version", action="version", version="%(prog)s "+str(version))
    parser.add_argument("-d", "--debug",
        action="store_true", dest="debug", default=False, 
        help='debug (default: %(default)s)')
    parser.add_argument("--drop",
        action="store_true", dest="drop", default=False, 
        help='drop table before create (default: %(default)s)')
    options = parser.parse_args()
    if options.file == None or options.regexp == None or options.sqlite == None:
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

    if (options.format != None) and (processor.testFormat(options.format) == False):
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




