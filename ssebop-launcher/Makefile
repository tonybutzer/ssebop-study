cat:
	cat Makefile


t:	config
	python3 launcher.py


config:
	cp ssebop-launcher.conf /wsefs/config


dirs:
	mkdir -p /wsefs/pipeline/todo
	mkdir -p /wsefs/pipeline/pending
	mkdir -p /wsefs/pipeline/complete
	mkdir -p /wsefs/pipeline/terminated
	mkdir -p /wsefs/pipeline/log
