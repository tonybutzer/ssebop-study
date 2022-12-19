#! /bin/bash

rename -v eta etf eta*

for file in Dockerfile Makefile *.py ; do echo $file; sed -i 's/eta/etf/g' $file; done

sed -i 's/eta/etf/g' Makefile 



