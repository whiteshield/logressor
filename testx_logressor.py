import logressor
import unittest
import StringIO
import sys
import subprocess

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

if __name__ == '__main__':
    unittest.main()
