.PHONY: clean data

data: 
	python ./src/data/scrape_runs_vods.py
	python ./src/data/scrape_donations.py
	python ./src/data/create_vod_run_tables.py
	python ./src/data/scrape_runner_twitter.py

clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete