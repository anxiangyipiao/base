

from scrapy.cmdline import execute
import os
import sys

if __name__ == '__main__':


    path = os.path.dirname(os.path.abspath(__file__)) 
    sys.path.append(path) 
    execute(['scrapy', 'crawl', 'sizebid_zhaobiao'])



