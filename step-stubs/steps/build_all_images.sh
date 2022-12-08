#! /bin/bash

for  mdir in `find . -type d -maxdepth 1 -mindepth 1`; do cd $mdir ; make build; done
