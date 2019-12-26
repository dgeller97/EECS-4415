How to run part A:
1) open 2 terminals in file location
2) For terminal 1: 
	- docker run -it -v $PWD:/app --name twitter -w /app python bash
	– pip install -U git+https://github.com/tweepy/tweepy.git
	– python twitter_app.py
3) For terminal 2:
	– docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415
	– spark-submit spark_app.py

How to run part B:
1) open 2 terminals in file location
2) For terminal 1: 
	- docker run -it -v $PWD:/app --name twitter -w /app python bash
	– pip install -U git+https://github.com/tweepy/tweepy.git
	– python twitter_app.py
3) For terminal 2:
	– docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415
	- pip install nltk
	- python (go into python bash)
		- import nltk
		- nltk.download('vader_lexicon')
		- exit() or ctrl+D 
	– spark-submit sparkSentiment_app.py

How to run graph.py:
	- open new cmd (or terminal) on local and go to file location
	- py graph.py (or python graph.py)
 
	