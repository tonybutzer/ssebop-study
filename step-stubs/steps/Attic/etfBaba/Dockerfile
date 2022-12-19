from pipe-runner-base

run groupadd -r -g 1000 ec2-user && useradd -r -u 1000 -g ec2-user ec2-user

run mkdir /etfBaba && mkdir /wsefs

copy etfBaba.* /etfBaba/
run chown -R ec2-user /etfBaba

workdir /etfBaba

user ec2-user
