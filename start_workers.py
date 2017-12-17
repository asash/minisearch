import sys
import subprocess
from utils import mkdir_p
import os

if len(sys.argv) > 1:
    QUEUE_URL = sys.argv[1]
else:
    QUEUE_URL = 'http://localhost:8888'

dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(dir)

N_WORKERS = 3 

mkdir_p('./logs/')

for i in range(N_WORKERS):
    stderr_log = open('./logs/worker-{}-stderr.log'.format(i), 'a')
    stdout_log = open('./logs/worker-{}-stdout.log'.format(i), 'a')
    subprocess.Popen(['python3', './crawler-worker.py', QUEUE_URL], 
                                            stderr=stderr_log,
                                            stdout=stdout_log)
