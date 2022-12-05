<!--ts-->
* [Design](#design)
* [ssebop-study](#ssebop-study)
* [BUGS](#bugs)

<!-- Created by https://github.com/ekalinin/github-markdown-toc -->
<!-- Added by: ec2-user, at: Mon Dec  5 13:11:18 UTC 2022 -->

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
