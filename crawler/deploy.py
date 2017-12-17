import aws
import os
import config
import subprocess
import shlex

def run_master_node():
    print("waiting for running of the master node...")
    master_node = aws.get_instance(config.MASTER_NODE_TYPE)
    print("master node run on ip {}".format(master_node.public_ip_address))
    return master_node


def run_worker_nodes(n=config.WORKER_NODES):
    print("waiting for running of the worker nodes...")
    worker_nodes = aws.get_instances(config.WORKER_NODE_TYPE, config.WORKER_NODES)
    for worker_node in worker_nodes:
        print( "worker node run on ip {}".format(worker_node.public_ip_address))
    return worker_nodes


def shell(command):
    print('executing "{}"'.format(command))
    subprocess.check_call(shlex.split(command))

def ssh_shell(ip, command):
    print('executing "{}" on host {}'.format(command, ip))
    cmd = "ssh -o StrictHostKeyChecking=no -i{}  {}@{} {}".format(
                                config.SSH_KEY,
                                config.SSH_USERNAME, 
                                ip, 
                                command)
    shell(cmd)



    

def deploy_code(node):
    code_dir = get_project_dir()
    cmd = 'scp -r -o StrictHostKeyChecking=no -i {} {} {}@{}:~/minisearch'\
                .format(config.SSH_KEY,
                                 code_dir,
                                 config.SSH_USERNAME,
                                 node.public_ip_address)
    shell(cmd)

def configure_python(node):
    ssh_shell(node.public_ip_address, "sudo bash ~/minisearch/deploy/setup_python.sh")

def start_urlqueue(node):
    ssh_shell(node.public_ip_address, "python3 ~/minisearch/start_queue.py")

def start_worker(node, queue_ip):
    cmd = "python3 ~/minisearch/start_workers.py http://{}:8888"\
                                                    .format(queue_ip)
    ssh_shell(node.public_ip_address, cmd)
             



def setup_graphite(node):
    ssh_shell(node.public_ip_address, 
            "sudo bash ~/minisearch/deploy/setup_graphite.sh")
    

def get_project_dir():
    return os.path.dirname(os.path.abspath(__file__))


