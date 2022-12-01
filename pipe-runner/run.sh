#! /bin/bash

source /wsefs/miniconda3/etc/profile.d/conda.sh \
	&& conda activate ssebop-study \
	&& conda env list \
	&& python3 /data/ec2-user/home/opt/ssebop-study/pipe-runner/pipe-runner.py
