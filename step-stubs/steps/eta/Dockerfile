from pipe-runner-base

run groupadd -r -g 1000 ec2-user && useradd -r -u 1000 -g ec2-user ec2-user

run mkdir /eta && mkdir /wsefs

copy eta.* /eta/
run chown -R ec2-user /eta

workdir /eta

user ec2-user
