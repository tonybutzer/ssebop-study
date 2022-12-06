from pipe-runner-base

run mkdir /normToGrid && mkdir /wsefs

copy normToGrid.* /normToGrid/

workdir /normToGrid

run groupadd -r -g 1000 ec2-user && useradd -r -u 1000 -g ec2-user ec2-user

user ec2-user
