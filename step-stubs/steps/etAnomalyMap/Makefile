cat:
	cat Makefile


# lowercase in docker world
Image=etanomalymap 

build:
	docker build -t ${Image} .


run:
	docker run -it ${Image} bash

t:
	docker run -it -v /wsefs:/wsefs ${Image} python3 /etAnomalyMap/etAnomalyMap.py

log:
	tail /wsefs/pipeline/log/norm*.log

publish:
	(cd /home/ec2-user/opt/ssebop-study; make publish)
