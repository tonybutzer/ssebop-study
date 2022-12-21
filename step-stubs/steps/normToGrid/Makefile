cat:
	cat Makefile


# lowercase in docker world
Image=normtogrid 

build:
	docker build -t ${Image} .


run:
	docker run -it -v /wsefs:/wsefs ${Image} bash

t:
	docker run -it -v /wsefs:/wsefs ${Image} python3 /normToGrid/normToGrid.py

log:
	tail /wsefs/pipeline/log/norm*.log

publish:
	(cd /home/ec2-user/opt/ssebop-study; make publish)
