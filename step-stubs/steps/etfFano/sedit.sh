#! /bin/bash

rename -v jobTemplate etfFano jobT*

for file in Dockerfile Makefile *.py ; do echo $file; sed -i 's/jobTemplate/etfFano/g' $file; done

sed -i 's/jobtemplate/etffano/g' Makefile 

