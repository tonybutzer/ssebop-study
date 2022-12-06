cat:
	cat Makefile


t:
	./run.sh
	tail /wsefs/pipeline/log/pipe-runner.log

log:
	        tail /wsefs/pipeline/log/pipe-runner.log


# cron:
	# echo '*/5 * * * * ' `pwd`/run.sh > mycron
	# cat mycron
	# crontab mycron

service:
	./installService.sh

stop:
	sudo systemctl stop pipe-runner
	sudo systemctl status pipe-runner


stat:
	sudo systemctl status pipe-runner

d:
	python3 test_docker_wrangle_run.py
