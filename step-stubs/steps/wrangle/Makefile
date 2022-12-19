cat:
	cat Makefile


build:
	cp /wsefs/pipeline/config/wrangle.yaml .
	docker build -t wrangle .
	rm wrangle.yaml


run:
	docker run -it wrangle bash

t:
	docker run -it -v /wsefs:/wsefs wrangle python3 /wrangle/wrangle.py


publish:
	(cd /home/ec2-user/opt/ssebop-study; make publish)
