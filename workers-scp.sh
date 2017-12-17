for worker in `cat run/workers.txt`;
do
     scp -r $1 ec2-user@$worker:$2;
done

