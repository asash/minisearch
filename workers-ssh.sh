for worker in `cat run/workers.txt`;
do
     ssh ec2-user@$worker $1;
done

