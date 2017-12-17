sh workers-ssh.sh 'bash ./minisearch/kill_workers.sh'
sh workers-scp.sh crawler-worker.py ./minisearch/
sh workers-ssh.sh 'python3 ./minisearch/start_workers.py $1'
