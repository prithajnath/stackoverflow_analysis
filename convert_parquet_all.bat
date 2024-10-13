@echo off
python convert_parquet.py -i data\Badges.csv -o data\Badges.parquet
python convert_parquet.py -i data\Comments.csv -o data\Comments.parquet
python convert_parquet.py -i data\PostHistory.csv -o data\PostHistory.parquet
python convert_parquet.py -i data\PostLinks.csv -o data\PostLinks.parquet
python convert_parquet.py -i data\Posts.csv -o data\Posts.parquet
python convert_parquet.py -i data\Tags.csv -o data\Tags.parquet
python convert_parquet.py -i data\Users.csv -o data\Users.parquet
python convert_parquet.py -i data\Votes.csv -o data\Votes.parquet
echo All CSV files have been converted to Parquet.
pause