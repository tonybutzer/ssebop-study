from pipe-runner-base

run groupadd -r -g 1000 ec2-user && useradd -r -u 1000 -g ec2-user ec2-user

run mkdir /etAnomalyMap && mkdir /wsefs

copy etAnomalyMap.* /etAnomalyMap/
run chown -R ec2-user /etAnomalyMap

workdir /etAnomalyMap

user ec2-user
