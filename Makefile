.PHONY: clean data

data: 
	python ./src/data/scrape_runs_vods.py
	python ./src/data/scrape_donations.py

clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete