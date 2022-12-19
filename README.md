<!--ts-->
* [Design](#design)
* [Integration Actions - 12/09/2022](#integration-actions---12092022)
   * [Wrangle](#wrangle)
   * [Docker](#docker)
   * [Dry-Run Wrangle](#dry-run-wrangle)
* [Monitoring](#monitoring)
* [ssebop-study](#ssebop-study)
* [BUGS](#bugs)

<!-- Created by https://github.com/ekalinin/github-markdown-toc -->
<!-- Added by: ec2-user, at: Mon Dec 19 19:00:11 UTC 2022 -->

<!--te-->


# Design

- `ssebop-launcher` runs on ws-costanalyzer
	- starts the ssebop production machine - ws-butzer-dev for testing
	- creates a workorder in /wsefs/pipeline/todo/wo-date.yml
- `pipe-runner`
	- runs as a service - see Makefile
	- sleeps for 6 minutes in a loop
	- looks for wo in todo
	- runs the workorder pipeline - 5 steps
	- shuts it self off - using `sudo shutdown -h now`

# Integration Actions - 12/09/2022

- demo tmux - watch docker - watch logs - 0_start_pipeline_gabe
- where is the sample data?
- where is kirk's data - any progress?
- can we build a very simple notebook for:
    - looking at input data affines - samples lines shape extent etc as a pandas data frame - talk about data compression - int16 ...
    - a notebook that does etfFano - with a clean set of functions and classes

- Integration Actions - 12/06/2022
## Wrangle
- add logging
- add timing of wget and unzip - about 12 minutes now
- optimize with parallel downloads - tbd
- discuss data locations
	- /wsefs/pipeline/sdata - or wdata for wrangle
	- /wsefs/pipeline/ndata - for snap to grid normalized data ?
## Docker

1. Build a base image - gabe
	- steal from tony's repo
2. Study step/wrangle Docker file from git - here:
	- https://github.com/tonybutzer/ssebop-study/tree/main/step-stubs/steps/wrangle
3. Study pipe-runner - gabe
4. Study ssebop-launcher - runs on a priviledged machine - costanalyzer
5. Discuss actions for thursday meeting - steffi


## Dry-Run Wrangle
- clean up sdata dir
- start the service
- shutdown the machine
- run ssebop-launcher
	- 12 minutes later we should have temperature data
		- 10 minutes to download
		- 2 minutes to unzip

# Monitoring
```
cdl

cat Makefile

cat:
        cat Makefile


clear:
        ./clearLogs.sh


tail:
        tail *.log | grep started


pipe:
        cat pipe-runner.log | grep start_


watch:
        watch -n 4 make pipe

dockers:
        watch docker ps

# tails last line of each logfile
one:
        ./wlogs.sh


sdata:
         watch -n 4 ls -lh /wsefs/pipeline/sdata/*.zip
```
# ssebop-study
ssebop-study

# BUGS

since devops gives us a super-small /root partition - docker builds easily run out of space.

- did not want ti use an nfs mount
- so used the data disk by adding a 

- see /etc/docker/daemon.json on kul's machine

```
$ cat /etc/docker/daemon.json
{
  "data-root": "/data/docker"
}


## SSEBOP PRODUCTION

### Steps

1. Download and wrangle LST and NDVI/NDWI data from DSS website
2. create ETF using new FANO method
3. create corrected ETf with BABA method
4. create ETa using reference ET --> output geotiff
5. create ET anomaly map (ETa/median ETa)  --> output graphic

> this would be the operational version that is processed every 2nd, 12th, and 22nd of the month

## Actions

### 1. Understanding The Inputs

#### LST
- `build ssebop-study conda env`
- fix makefile and readme etc
- create a notebook to explore

## Performance Optimizing

## Refining the requirements - can we go smaller than the globe

## OPERATIONAL HARDENING

- code simplification and refactoring
- organized pip installable library
- pipeline framework - baked off
	- kendro
	- airflow
	- panel pipelines


### A note about style and zen from pep20

 - Beautiful is better than ugly.
 - Explicit is better than implicit.
 - Simple is better than complex.

1. Readability counts.
2. Errors should never pass silently.
3. In the face of ambiguity, refuse the temptation to guess.
4. Now is better than never.
	- Although never is often better than *right* now.
5. If the implementation is easy to explain, it may be a good idea.

## Engineering

#### TOC STuff

- https://github.com/ekalinin/github-markdown-toc
