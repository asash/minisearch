from deploy import *
import sys
import config

queue_ip = sys.argv[1]
n = config.WORKER_NODES
if len (sys.argv) > 2:
    n = int(sys.argv[2])


if __name__ == '__main__':
    workers = run_worker_nodes(n) 
    for worker in workers:
        deploy_code(worker) 
        configure_python(worker)
        start_worker(worker, queue_ip)
