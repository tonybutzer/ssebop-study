from pipe-runner-base

run groupadd -r -g 1000 ec2-user && useradd -r -u 1000 -g ec2-user ec2-user

run mkdir /jobTemplate && mkdir /wsefs

copy jobTemplate.* /jobTemplate/
run chown -R ec2-user /jobTemplate

workdir /jobTemplate

user ec2-user
