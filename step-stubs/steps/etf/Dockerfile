from pipe-runner-base

run groupadd -r -g 1000 ec2-user && useradd -r -u 1000 -g ec2-user ec2-user

run mkdir /etf  \
    && mkdir /wsefs  \
    && mkdir /home/ec2-user \
    && apt-get update \
    && apt-get install nano \
    && apt-get install make

env tony 1.1
copy ssebopLib/ /etf/ssebopLib/
copy etf.* /etf/
run chown -R ec2-user /etf /home 

workdir /etf

user ec2-user
run (cd /etf/ssebopLib; make build) && cp /etf/ssebopLib/ssebopLib/input_manager.py /etf/etf.py
