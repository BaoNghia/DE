# How to create a PostgreSQL stock database using Python

This code example will help you create your own stock pricing database using PostgreSQL and Python.  The code will create tables:

daily_prices:  A table containing daily stock pricing information (pkey:  symbol, date)


## Database Requirements

Download and install [PostgreSQL](https://www.postgresql.org/)

Download and install [pgAdmin](https://www.pgadmin.org/)


## Python Requirements

First, go to main directory and create new virtual environment:

`$ virtualenv  ./venv -p /usr/bin/python3`

Activate the new virtual environment

`$ source ./venv/bin/activate`

Install packages

`$ pip install -r requirements.txt`

And the next step you need to go to **Stock-Database** and create database in PostgreSQL

```bash
$ cd Stock-Database
$ python create_db.py
```

Edit your python path in **make_dataset.sh** to create cronjob.

Finally, create cronjob to shedule time to crawl and import to PostgreSQL database

```bash
$ cd
$ crontab -e
# insert texts below and save
0 12 * * * cd ~/DE/Stock-Database && bash make_dataset.sh
```

This script will crawl and add to database automatically at 12:00 pm every day.
