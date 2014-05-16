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
import unittest
import subprocess

class logressor:
    """
    Log manipulator class
    """
    def __init__(self, inputargs):
        """Variable initialization."""
        self.silent=False
        self.debug=False
        self.list=False
        self.file=''
        self.regexp=''
        self.format={}
        self.sqlite=''
        self.table='data'
        self.drop=False
        self.vacuum=False
        self.separator=''
        self.sql=''
        self.logType=''
        self.logTypes=[]
        self.fields=[]
        self.timestamps={}
        self.reals=[]
        self.stdIn=False
        self.remove=[]
        prog="logressor.py"
        version=0.5
        description = prog+" "+str(version) + "\n" +"""This script is able to convert log files to sqlite format based 
    on regexp named group method."""
        epilog = """
    Sample usage:
     Process sample 1)
      python logressor.py \\
        --file sample/s.log \\
        --regexp \"^(?P<v1>.{15})\s+(?P<v2>\S+)\s+(?P<v3>\S+)*\" \\
        --sqlite sample/output.sqlite \\
        --format \"{'v1':{'type':'timestamp','format':'%b %d %H:%M:%S'},'v2':'real'}\" \\
        --remove \"v3\" \\
        --drop

     Process sample 2) (copy user.dict-sample to user.dict!)
      python logressor.py --file sample/s.log --logtype sample --sqlite sample/output.sqlite --drop

     Process appache access.log
      cat sample/access.log | python logressor.py --logtype apache --sqlite sample/output.sqlite --drop

     List defined log types
      python logressor.py --list

    """
        parser = argparse.ArgumentParser(description=description, epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter, prog=prog)
        parser.add_argument("--file",
            metavar="FILE", action="store",
            help="log file to work on (or standard input, if parameter not given)")
        parser.add_argument("--regexp",
            metavar="REGEXP", action="store",
            help="regexp with named groups to convert log values")
        parser.add_argument("--separator",
            metavar="REGEXP", action="store",
            help="regexp to separate multiline logs")
        parser.add_argument("--format",
            metavar="FORMAT", action="store", 
            help="format of named groups in parseable dict")
        parser.add_argument("--remove",
            metavar="FIELDLIST", action="store", 
            help="comma separated list of removabel fields")
        parser.add_argument("--logtype",
            metavar="TYPE", action="store", 
            help="predefined log type from logressor.dict or user.dict")
        parser.add_argument("--list",
            action="store_true", dest="list", default=False,
            help="list predefined log types from logressor.dict or user.dict")
        parser.add_argument("--sqlite",
            metavar="SQLITEFILE", action="store",
            help="the result sqlite file name (or standard output, if parameter not given)")
        parser.add_argument("--table",
            metavar="TABLE", action="store", nargs="?",
            help="the table name in sqlite database")
        parser.add_argument("-v", "--version", action="version", version="%(prog)s "+str(version))
        parser.add_argument("-d", "--debug",
            action="store_true", default=False, 
            help='debug (default: %(default)s)')
        parser.add_argument("--drop",
            action="store_true", default=False, 
            help='drop table before create (default: %(default)s)')
        parser.add_argument("--vacuum",
            action="store_true", default=False, 
            help='vacuum the database after inserts')
        parser.add_argument("--silent",
            action="store_true", default=False, 
            help='silent mode')
        options = parser.parse_args(args=inputargs)
        if (options.regexp != None or options.format != None ) and options.logtype != None:
            parser.error("Can't set regexp/format and logtype in same time! It's ambigous!")
            sys.exit()
        if options.regexp == None and options.logtype == None and options.list == False:
            parser.error("Incorrect number of arguments! Regexp/logtype/list required!")
            sys.exit()
        if options.list == None and options.sqlite == None:
            parser.error("Incorrect number of arguments! Need a sqlite file!")
            sys.exit()
        if options.file != None and not os.path.isfile(options.file):
            parser.error(options.file + " does not exists!")
            sys.exit()
        if options.file != None and not os.access(options.file, os.R_OK):
            parser.error(options.file + " access denied!")
            sys.exit()
        if options.sqlite != None and os.path.isfile(options.sqlite) and not os.access(options.sqlite, os.W_OK):
            parser.error(options.sqlite + " access denied!")
            sys.exit()
        if options.regexp!=None:
            try:
                re.compile(options.regexp)
            except re.error, e:
                parser.error("REGEXP not valid: " + str(e))
                sys.exit()


        if (options.format != None) and (self.testFormat(options.format) == False):
            parser.error("FORMAT can't evaluate to dict!")
            sys.exit()
        if os.path.isfile(os.path.abspath(os.path.dirname(sys.argv[0])) + os.sep + 'logressor.dict') :
            logressorFormat = open(os.path.abspath(os.path.dirname(sys.argv[0])) + os.sep + 'logressor.dict').read()
            if self.testDictFile(logressorFormat) == False:
                parser.error("logressor.dict can't evaluate to dict!")
                sys.exit()
        if os.path.isfile(os.path.abspath(os.path.dirname(sys.argv[0])) + os.sep + 'user.dict') :
            logressorFormat = open(os.path.abspath(os.path.dirname(sys.argv[0])) + os.sep + 'user.dict').read()
            if self.testDictFile(logressorFormat) == False:
                parser.error("user.dict can't evaluate to dict!")
                sys.exit()
        if (options.logtype != None) and (self.testLogType(options.logtype) == False):
            parser.error("Logtype not found in predefined log types!")
            sys.exit()

        self.setInputSilent(options.silent)
        self.setInputFile(options.file)
        self.setInputRegexp(options.regexp)
        self.setInputRemove(options.remove)
        self.setInputFormat(options.format)
        self.setInputSqlite(options.sqlite)
        self.setInputTable(options.table)
        self.setInputDrop(options.drop)
        self.setInputVacuum(options.vacuum)
        self.setInputSeparator(options.separator)
        self.setInputLogType(options.logtype)
        self.setInputDebug(options.debug)
        self.setInputList(options.list)
        self.process()


    def setInputSilent(self, silent):
        """Set the class variable silent."""
        self.silent=silent
    def setInputDebug(self, debug):
        """Set the class variable debug."""
        self.debug=debug
    def setInputList(self, list):
        """Set the class variable list."""
        self.list=list
    def setInputDrop(self, drop):
        """Set the class variable drop."""
        self.drop=drop
    def setInputVacuum(self, vacuum):
        """Set the class variable vacuum."""
        self.vacuum=vacuum
    def setInputSeparator(self, separator):
        """Set the class variable separator."""
        self.separator=separator
    def setInputFile(self, file):
        """Set the class variable file."""
        if file == None:
            self.stdIn=True
        else:
            self.file=file
    def setInputRegexp(self, regexp):
        """Set the class variable regexp."""
        self.regexp=regexp
    def setInputRemove(self, remove):
        """Set the class variable remove."""
        if remove != None:
            self.remove=re.compile('\s*,\s*').split(remove)
    def setInputFormat(self, format):
        """Set the class variable format."""
        if format != None:
            self.format=format
    def setInputLogType(self, logtype):
        """Set the class variable logType."""
        if logtype != None:
            self.logType=logtype
    def setInputSqlite(self, sqlite):
        """Set the class variable sqlite."""
        self.sqlite=sqlite
    def setInputTable(self, table):
        """Set the class variable table."""
        if table != None:
            self.table=table
    def showOptions(self):
        """Display configuration options."""
        justify = 14
        print ""
        print "debug: ".rjust(justify) + str(self.debug)
        print "list: ".rjust(justify) + str(self.list)
        print "file: ".rjust(justify) + str(self.file)
        print "stdIn: ".rjust(justify) + str(self.stdIn)
        print "regexp: ".rjust(justify) + str(self.regexp)
        print "separator: ".rjust(justify) + str(self.separator)
        print "format: ".rjust(justify) + str(self.format)
        print "logType: ".rjust(justify) + str(self.logType)
        print "logTypes: ".rjust(justify) + str(self.logTypes)
        print "sqlite: ".rjust(justify) + str(self.sqlite)
        print "table: ".rjust(justify) + str(self.table)
        print "drop: ".rjust(justify) + str(self.drop)
        print "vacuum: ".rjust(justify) + str(self.vacuum)
        print "fields: ".rjust(justify) + str(self.fields)
        print "remove: ".rjust(justify) + str(self.remove)
        print "timestamps: ".rjust(justify) + str(self.timestamps)
        print "reals: ".rjust(justify) + str(self.reals)
        print "sql: ".rjust(justify) + str(('\n'+" "*justify).join(self.sql.split('\n')))
        print ""
    def process(self):
        """Start the program!"""
        if not self.list:
            if self.logType!='':
                self.loadLogTypes()
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
                if self.debug:
                    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" prepare database start"
                self.connection = sqlite3.connect(self.sqlite)
                cursor = self.connection.cursor()    
                #insert into d values (null, '11:10', 'front', 'method', 'POST', 20, 20);
                sqlCommands=self.sql.split(';')
                for query in sqlCommands:
                    cursor.execute(query)
                self.connection.commit()
                if self.debug:
                    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" prepare database end"
                self.parseFile()
            except sqlite3.Error, e:
                sys.stderr.write("EM01 Sqlite error: %s\n" % e.args[0])
                sys.exit()
            finally:
                if self.connection:
                    self.connection.close()
            if self.debug:
                print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" process end"
            if not self.silent:
                print ('stdin' if self.stdIn else self.file) + " loaded into " + self.sqlite
                print "try it: sqlite3 "+self.sqlite+" \"select "+','.join(self.fields)+" from "+ self.table+" limit 10\""
        elif self.list :
            print "logtypes:"
            for logType in self.logTypes:
                print " - " + logType

    def loadLogTypes(self):
        if os.path.isfile(os.path.abspath(os.path.dirname(sys.argv[0])) + os.sep + 'logressor.dict') :
            logressorFormat = open(os.path.abspath(os.path.dirname(sys.argv[0])) + os.sep + 'logressor.dict').read()
            self.loadFormatsFromFile(logressorFormat)

        if os.path.isfile(os.path.abspath(os.path.dirname(sys.argv[0])) + os.sep + 'user.dict') :
            logressorFormat = open(os.path.abspath(os.path.dirname(sys.argv[0])) + os.sep + 'user.dict').read()
            self.loadFormatsFromFile(logressorFormat)

    def loadFormatsFromFile(self,dict):
        try:
            dictFile=ast.literal_eval(dict)
            for logTypeName,logTypeValue in dictFile.iteritems():
                if logTypeName==self.logType:
                    self.regexp=logTypeValue.get('regexp')
                    self.format=str(logTypeValue.get('format')) if logTypeValue.get('format') else ''
                    self.multiline=True if logTypeValue.get('multiline') and logTypeValue.get('multiline').lower()=='true' else False
                    self.remove=re.compile('\s*,\s*').split(logTypeValue.get('remove')) if logTypeValue.get('remove') else []
        except Exception, e:
            sys.stderr.write("ED04 Dict read error: %s\n" % e.args[0])

    def loadFields(self):
        regexp1=re.compile("\?P<(\w+)>")
        results=regexp1.findall(self.regexp) 
        if results:
            self.fields=[]
            for field in results:
                if not field in self.remove  :
                    self.fields.append(field)
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
                if fieldType=='real':
                    self.reals.append(fieldName)
                if not fieldType in ('real', 'text', 'integer'):
                    fieldType='text'
            else:
                fieldType='text'
            self.sql = self.sql + "  " + fieldName + " " + fieldType + ", \n"
        self.sql=self.sql[0:-3] + '\n);\n'
        if self.vacuum:
            self.sql=self.sql+"vacuum;"


    def testDictFile(self, dict):
        try:
            success=True
            dictFile=ast.literal_eval(dict)
            for logTypeName,logTypeValue in dictFile.iteritems():
                self.logTypes.append(logTypeName)
                if logTypeValue.get('regexp'):
                    try:
                        re.compile(logTypeValue.get('regexp'))
                    except re.error, e:
                        sys.stderr.write("ED01 REGEXP not valid in " + logTypeName + "\n")
                        success = False
                else :
                    sys.stderr.write("ED02 Dict format error: regexp missing!\n")
                    success = False
                if logTypeValue.get('format'):
                    success = success and self.testFormat(str(logTypeValue.get('format')))
            return success
        except Exception, e:
            sys.stderr.write("ED03 Dict read error: %s\n" % e.args[0])
            return False

    def testLogType(self, logtype):
        if not logtype in self.logTypes:
            return False
        else:
            return True       

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
                        sys.stderr.write("EF01 Missing format type in " + fieldName + "\n")
                        success=False
                    if attributes.get('format') and fieldType == 'timestamp':
                        fieldFormat=attributes.get('format')
                        fieldType = 'text'
                elif type(attributes) is str:
                    fieldType=attributes
                else:
                    success=False
                    sys.stderr.write("EF02 Unknown format in " + fieldName + "\n")
                if not fieldType in ('real', 'text', 'integer'):
                    success=False
                    sys.stderr.write("EF03 Not valid field type in " + fieldName + "\n")
            return success
        except Exception, e:
            sys.stderr.write("EF04 Format read error: %s\n" % e.args[0])
            return False

    def parseFile(self):
        if self.debug:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" parse start"
        linesOK=0;
        linesFail=0
        cursor = self.connection.cursor()
        if self.separator != '':
            #multiline log process
            regex1 = re.compile(self.regexp, re.DOTALL|re.MULTILINE)
        else:
            regex1 = re.compile(self.regexp)
        i=0
        if not self.stdIn:
            filePointer = open(self.file)
        else:
            filePointer = sys.stdin
        for line in filePointer:
            parse = line.strip()
            if len(parse)>0:
                r1 = regex1.search(parse)
                result_set = {}
                if r1:
                    if self.parseLine(r1, cursor):
                        linesOK+=1
                    else:
                        linesFail+=1
                else:
                    if not self.silent:
                        sys.stderr.write("EP02 Error in regexp process: "+line.strip()+"\n")
                        linesFail+=1
        if self.debug:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" parse end with total "+str(linesOK+linesFail)+" lines (OK: "+str(linesOK)+"/ Fail: "+str(linesFail)+")"
        if self.debug:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" commit start"
        self.connection.commit()
        if self.debug:
            print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+" commit end"
        if not self.stdIn:
            filePointer.close()

    def parseLine(self, r1, cursor):
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
                elif f in self.reals:
                    value=0
                    try:
                        value=float(resultDict.get(f))
                    except Exception, e:
                        pass
                    valueList.append(value)
                else:
                    valueList.append(resultDict.get(f))
            else:
                valueList.append(None)
        insert="insert into "+self.table+" ("+(",".join(str(e) for e in fieldList))+") values ("+",".join("?" for i in range(0, len(fieldList)))+")"
        try:
            cursor.execute(insert, valueList)
            return True
        except Exception, e:
            sys.stderr.write("EP01 Sqlite error: %s\n" % e.args[0])
            return False

class logressor_test(unittest.TestCase):

    def testLogressorList(self):
        ferr=open('stderr.tmp','w')
        fout=open('stdout.tmp','w')
        subprocess.call("python logressor.py --list", shell=True, stderr = ferr, stdout= fout)
        fout.close()
        ferr.close()
        serr = open('stderr.tmp').read()
        sout = open('stdout.tmp').read()
        assert len(serr.strip()) == 0

def test():
    unittest.main()

if __name__ == "__main__":
    logressor(sys.argv[1:])




