import psycopg2
from psycopg2 import Error, sql
from configparser import ConfigParser
import logging
from vivino_scraper import *
from datetime import date


def __get_config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config


db_config = __get_config()


class MetaSingleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(MetaSingleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Database(metaclass=MetaSingleton):
    def __init__(self):
        self.conn = None  # type: psycopg2.connection
        self.cur = None  # type: psycopg2.cursor
        self.config = db_config
        self.create_connection()

    def __del__(self):
        self.close_connection()

    def create_connection(self):
        try:
            # raise exception is config not set
            self.conn = psycopg2.connect(**self.config)
            self.cur = self.conn.cursor()
            print("DB INFO")
            print(self.conn.get_dsn_parameters(), "\n")
        except (Exception, Error) as error:
            print("ERROR PostgreSQL", error)
        finally:
            if self.conn:
                print("DB CONNECTION SUCCEEDED\n")

    def close_connection(self):
        if self.conn:
            self.conn.commit()
            self.cur.close()
            self.conn.close()
            print("DB CONNECTION CLOSED")

    def create_wine_table(self):
        field_names = {'name': 'VARCHAR(255)',
                       'price_new': 'FLOAT',
                       'price_old': 'FLOAT',
                       'rating': 'FLOAT',
                       'shop': 'VARCHAR(255)',
                       'url': 'VARCHAR(255)',
                       'updated': 'VARCHAR(255)',
                       'posted': 'BOOLEAN'
                       }
        query = """CREATE TABLE {table} ({fields})""".format(
            table='wines',
            fields=', '.join(
                [' '.join([item[0], item[1]]) for item in field_names.items()]
            ))
        self.cur.execute(query)
        query = """ALTER TABLE wines ADD CONSTRAINT uc UNIQUE (name)"""
        self.cur.execute(query)
        self.conn.commit()

    def delete_wine_table(self):
        query = """DROP TABLE {table}""".format(
            table='wines'
        )
        logging.info(query)
        self.cur.execute(query)
        self.conn.commit()

    def insert_wine(self, item):
        try:
            field_names = ['name',
                           'price_new',
                           'price_old',
                           'rating',
                           'shop',
                           'url',
                           'updated',
                           'posted',
                           ]
            # query = sql.SQL("""INSERT INTO wines ({fields}) VALUES ({values})""").format(
            #     fields=sql.SQL(', ').join(map(sql.Identifier, field_names)),
            #     values=sql.SQL(', ').join(map(sql.Literal, [item[f] for f in field_names])),
            # )
            query = sql.SQL("""INSERT INTO wines ({fields}) VALUES ({values})
            ON CONFLICT ON CONSTRAINT uc DO UPDATE SET {updates}""").format(
                fields=sql.SQL(', ').join(map(sql.Identifier, field_names)),
                values=sql.SQL(', ').join(map(sql.Literal, [item[f] for f in field_names])),
                updates=sql.SQL(', ').join(
                    sql.Composed([sql.Identifier(f), sql.SQL(" = "), sql.Literal(item[f])]) for f in field_names
                ),
            )
            self.cur.execute(query)
            logging.info(f"Successfully inserted wine: {item['name']}")
        except BaseException as e:
            logging.debug(f"Error in insert_wine(): {e}")
        finally:
            self.conn.commit()

    def get_good_wines(self):
        query = sql.SQL("""SELECT * FROM wines WHERE rating >= 4.0""")
        self.cur.execute(query)
        wines = self.cur.fetchall()
        self.output = []
        for wine in wines:
            self.output.append('\n'.join(str(f) for f in wine))
        return self.output

    def count_new_wines(self):
        query = sql.SQL("""SELECT name FROM wines WHERE rating = -1""")
        self.cur.execute(query)
        wines = self.cur.fetchall()
        counter = 0
        for wine in wines:
            print(wine)
            counter += 1
        print(counter)

    def rate_new_wines(self):
        query = sql.SQL("""SELECT name FROM wines WHERE rating = -1""")
        self.cur.execute(query)
        wines = self.cur.fetchall()
        for wine in wines:
            rating = get_vivino_rating(wine)
            query = sql.SQL("""UPDATE wines SET rating = {rt} WHERE name = {name}""").format(
                name=sql.Literal(wine),
                rt=sql.Literal(rating),
            )
            self.cur.execute(query)
            self.conn.commit()

    def clean_up(self):
        query = sql.SQL("""SELECT COUNT(name) FROM wines WHERE updated != {date}""").format(
            date=sql.Literal(date.today().strftime("%Y-%m-%d"))
        )
        self.cur.execute(query)
        outdated_wines_count = self.cur.fetchall()
        logging.info(f"Outdated wines count: {outdated_wines_count}")

        query = sql.SQL("""DELETE FROM wines WHERE updated != {date}""").format(
            date=sql.Literal(date.today().strftime("%Y-%m-%d"))
        )
        self.cur.execute(query)
        logging.info(f"DELETED {outdated_wines_count} outdated wines")
        self.conn.commit()

