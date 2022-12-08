from pipe-runner-base

run groupadd -r -g 1000 ec2-user && useradd -r -u 1000 -g ec2-user ec2-user

run mkdir /etfFano && mkdir /wsefs

copy etfFano.* /etfFano/
run chown -R ec2-user /etfFano

workdir /etfFano

user ec2-user
