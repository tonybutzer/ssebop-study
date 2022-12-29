from pipe-runner-base

run groupadd -r -g 1000 ec2-user && useradd -r -u 1000 -g ec2-user ec2-user

run mkdir /baba  \
    && mkdir /wsefs  \
    && mkdir /home/ec2-user \
    && apt-get update \
    && apt-get install nano \
    && apt-get install make

env tony 1.2
copy baba.* /baba/
run chown -R ec2-user /baba /home 

workdir /baba

user ec2-user
