#! /bin/bash

sudo cp pipe-runner.service /lib/systemd/system/pipe-runner.service

sudo systemctl daemon-reload
sudo systemctl enable pipe-runner.service
sudo systemctl start pipe-runner.service
sudo systemctl status pipe-runner.service

