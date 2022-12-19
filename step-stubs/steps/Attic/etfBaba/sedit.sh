#! /bin/bash

rename -v jobTemplate etfBaba jobT*

for file in Dockerfile Makefile *.py *.yaml; do echo $file; sed -i 's/jobTemplate/etfBaba/g' $file; done

sed -i 's/jobtemplate/etfbaba/g' Makefile 

