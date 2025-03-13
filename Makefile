SHELL := /bin/bash

pre-commit:
	source venv/bin/activate &&\
	pip freeze &&\
	pre-commit install &&\
	pre-commit run --all-files

