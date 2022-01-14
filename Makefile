black:  ## Black format Python and Jupyter Notebook/Lab files
	find . -type f -name "*.ipynb" | xargs jblack --line-length=100;
	find . -type f -name "*.py" | xargs black --line-length=100;


clean:  ## Remove .DS_Store and __pycache__/
	find . -type d -name "__pycache__" | xargs rm -r;
	find . -type f -name ".DS_Store" | xargs rm;

