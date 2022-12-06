cat:
	cat Makefile


build:
	cp ~/.config/wrangle.yaml .
	docker build -t wrangle .
	rm wrangle.yaml


run:
	docker run -it wrangle bash

t:
	docker run -it -v /wsefs:/wsefs wrangle python3 /wrangle/wrangle.py
