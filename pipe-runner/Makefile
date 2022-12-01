cat:
	cat Makefile


t:
	./run.sh
	tail /wsefs/pipeline/log/pipe-runner.log

log:
	        tail /wsefs/pipeline/log/pipe-runner.log


cron:
	echo '*/5 * * * * ' `pwd`/run.sh > mycron
	cat mycron
	crontab mycron
