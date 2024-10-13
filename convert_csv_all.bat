@echo off
python app.py -s badges
python app.py -s comments
python app.py -s posthistory
python app.py -s postlinks
python app.py -s posts
python app.py -s tags
python app.py -s users
python app.py -s votes
echo All CSV files have been generated.