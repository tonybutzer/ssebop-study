cat:
	cat Makefile


# lowercase in docker world
Image=eta 

build:
	docker build -t ${Image} .


run:
	docker run -it ${Image} bash

t:
	docker run -it -v /wsefs:/wsefs ${Image} python3 /eta/eta.py

log:
	tail /wsefs/pipeline/log/norm*.log

publish:
	(cd /home/ec2-user/opt/ssebop-study; make publish)
