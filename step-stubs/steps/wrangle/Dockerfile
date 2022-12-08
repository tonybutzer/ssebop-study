from pipe-runner-base

run mkdir /wrangle && mkdir /wsefs

copy wrangle.* /wrangle/

workdir /wrangle

run groupadd -r -g 1000 ec2-user && useradd -r -u 1000 -g ec2-user ec2-user

run chown -R ec2-user /wrangle

user ec2-user
