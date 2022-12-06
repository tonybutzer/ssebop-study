cat:
	cat Makefile


t:
	./run.sh
	tail /wsefs/pipeline/log/pipe-runner.log

log:
	        tail -f /wsefs/pipeline/log/pipe-runner.log


service:
	./installService.sh

start:
	sudo systemctl enable pipe-runner
	sudo systemctl start pipe-runner
	sudo systemctl status pipe-runner

stop:
	sudo systemctl stop pipe-runner
	sudo systemctl status pipe-runner


stat:
	sudo systemctl status pipe-runner

d:
	python3 test_docker_wrangle_run.py


publish:
	(cd /home/ec2-user/opt/ssebop-study; make publish)
