from pipe-runner-base

run groupadd -r -g 1000 ec2-user && useradd -r -u 1000 -g ec2-user ec2-user

run mkdir /normToGrid && mkdir /wsefs

copy normToGrid.* /normToGrid/
run chown -R ec2-user /normToGrid

workdir /normToGrid

user ec2-user
