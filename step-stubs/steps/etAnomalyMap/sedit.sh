#! /bin/bash

rename -v jobTemplate etAnomalyMap jobT**

for file in Dockerfile Makefile *.py *.yaml; do echo $file; sed -i 's/jobTemplate/etAnomalyMap/g' $file; done

sed -i 's/jobtemplate/etanomalymap/g' Makefile 



