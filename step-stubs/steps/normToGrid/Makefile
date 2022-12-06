cat:
	cat Makefile


build:
	docker build -t normToGrid .


run:
	docker run -it normToGrid bash

t:
	docker run -it -v /wsefs:/wsefs normToGrid python3 /normToGrid/normToGrid.py


publish:
	(cd /home/ec2-user/opt/ssebop-study; make publish)
