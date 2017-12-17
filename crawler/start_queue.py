import subprocess
import sys
from utils import mkdir_p
import os

dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir)

mkdir_p('./logs/')

stderr_log = open('./logs/queue-stderr.log', 'a')
stdout_log = open('./logs/queue-stdout.log', 'a')
subprocess.Popen(['python3', './crawler-url-queue.py'] + sys.argv[1:], 
                                            stderr=stderr_log,
                                            stdout=stdout_log)
