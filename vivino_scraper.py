from bs4 import BeautifulSoup
import requests
import logging
import time


def get_vivino_rating(wine_name: str) -> float:
    while True:
        try:
            url = f"https://www.vivino.com/search/wines?q={wine_name}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'
            }
            page = requests.get(url, headers=headers)
            soup = BeautifulSoup(page.text, "html.parser")
            rating_div = soup.find('div', class_='text-inline-block light average__number')
            rating = float(rating_div.text.strip().replace(',', '.'))
            logging.info(f"SUCCESSFULLY GOT RATING for wine {wine_name}")
            return rating
        except BaseException as e:
            if page.status_code != 200:
                logging.error(f"VIVINO UNAVAILABLE status_code {page.status_code}\n SLEEP FOR 300 SEC")
                time.sleep(300)
            else:
                logging.error(f"ERROR in get_viviono_rating(): {e}.\nfor wine {wine_name}")
                return -2


def get_vivino_image(wine_name):
    url = f"https://www.vivino.com/search/wines?q={wine_name}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'
    }
    page = requests.get(url, headers=headers)
    if page.status_code != 200:
        logging.error(f"RATING FOR WINE {wine_name} NOT FOUND. COULDN'T CONNECT")
        return
    soup = BeautifulSoup(page.text, "html.parser")
    img_url = f"""http:{soup.find('img', class_='image').attrs['src']}"""
    img_jpg = requests.get(img_url).content
    return img_jpg


def get_vivino_tags(wine_name):
    pass
