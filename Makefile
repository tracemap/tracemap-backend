add-token:
	python crawler/helpers/addToken.py

run-crawler:
	python crawler/queuingCrawlers.py

test:
	pytest

start-uwsgi:
	uwsgi --ini wsgi-conf.ini

start-flask:
	python server.py runserver -h 0.0.0.0