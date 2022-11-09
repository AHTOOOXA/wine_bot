import datetime
import logging
import time
import config
import schedule
from aiogram import executor

import bot
import telergam_manager
from db_manager import Database
import parsing
import logging
db = Database()


def main():
    logging.basicConfig(level=logging.INFO)

    # REFACTOR
    if config.first_run:
        config.first_run = False
        db.create_wine_table()
    daily_routine()
    schedule.every(24).hours.do(daily_routine)
    while True:
        schedule.run_pending()


def daily_routine():
    parsing.scrap()
    db.rate_new_wines()
    db.clean_up()
    for wine in db.get_good_wines():
        telergam_manager.post_to_telegram(wine)


def test_f():
    pass


if __name__ == '__main__':
    main()
    # test_f()
