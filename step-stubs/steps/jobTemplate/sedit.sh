#! /bin/bash

rename -v jobTemplate eta jobT**

for file in Dockerfile Makefile *.py ; do echo $file; sed -i 's/jobTemplate/eta/g' $file; done

sed -i 's/jobtemplate/eta/g' Makefile 



