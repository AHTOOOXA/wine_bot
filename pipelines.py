import psycopg2
import logging
from db_manager import Database


class TestPipeline:
    def process_item(self, item, spider):
        print('TestPipeline')
        return item


class SavingToPostgresPipeline(object):
    def __init__(self):
        self.database = Database()

    # def create_connection(self):
    #     self.connection = psycopg2.connect(user="postgres",
    #                                        # пароль, который указали при установке PostgreSQL
    #                                        password="71130",
    #                                        host="127.0.0.1",
    #                                        port="5432",
    #                                        database="winebot_db")
    #     self.curr = self.connection.cursor()
    #     print("Информация о сервере PostgreSQL")
    #     print(self.connection.get_dsn_parameters(), "\n")
    #     # Выполнение SQL-запроса
    #     self.curr.execute("SELECT version();")
    #     # Получить результат
    #     record = self.curr.fetchone()
    #     print("Вы подключены к - ", record, "\n")
    #
    # def store_db(self, item):
    #     try:
    #         self.curr.execute(""" insert into wines (name, price_new, price_old, rating, shop, url, updated) values (%s, %s, %s, %s, %s, %s, %s)""", (
    #             item["name"],
    #             item["price_new"],
    #             item["price_old"],
    #             item["rating"],
    #             item["shop"],
    #             item["url"],
    #             item["updated"]
    #         ))
    #         print(f"Вставлен элемент: {item['name']}")
    #     except BaseException as e:
    #         print(e)
    #     finally:
    #         self.connection.commit()

    # def store_db(self, item):
    #     try:
    #         field_names = ['name',
    #                        'price_new',
    #                        'price_old',
    #                        'rating',
    #                        'shop',
    #                        'url',
    #                        'updated',
    #                        ]
    #         query = """INSERT INTO wines ({fields}) values ({values})""".format(
    #             fields=', '.join(field_names),
    #             values=', '.join([item[f] for f in field_names])
    #         )
    #         logging.info(query)
    #         print(query)
    #         # self.curr.execute(query)
    #         logging.info(f"Вставлен элемент: {item['name']}")
    #         print(f"Вставлен элемент: {item['name']}")
    #     except BaseException as e:
    #         print(e)
    #     finally:
    #         self.connection.commit()

    def process_item(self, item, spider):
        self.database.insert_wine(item)
        # self.store_db(item)
        # we need to return the item below as scrapy expects us to!
        return item
