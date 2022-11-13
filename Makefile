cat:
	cat Makefile


publish:
	git add .
	git commit -m 'git make publish - tony - 9/2022'
	git push


toctool:
	wget https://raw.githubusercontent.com/ekalinin/github-markdown-toc/master/gh-md-toc
	chmod a+x gh-md-toc
	sudo mv gh-md-toc /usr/local/bin


toc:
	gh-md-toc --insert --no-backup README.md
