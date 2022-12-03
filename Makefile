cat:
	cat Makefile


publish: toc
	#cat ~/token.txt
	git add .
	git commit -m 'git make publish - tony - 9/2022'
	git push


toctool:
	wget https://raw.githubusercontent.com/ekalinin/github-markdown-toc/master/gh-md-toc
	chmod a+x gh-md-toc
	sudo mv gh-md-toc /usr/local/bin


toc:
	gh-md-toc --insert --no-backup README.md


bashrc:
	cp /wsefs/KEYS/example_bashrc ~/.bashrc
	cp /wsefs/KEYS/example_profile ~/.profile

docker:
	sudo yum install docker
	sudo usermod -aG docker ${USER}
	sudo systemctl enable docker
	sudo systemctl start docker
	sudo cp .....
