import requests
import config


def post_to_telegram(message):
    url = f'https://api.telegram.org/bot{config.token}/sendMessage'
    try:
        response = requests.post(url, json={'chat_id': config.channel_id, 'text': message})
        print(response.text)
    except Exception as e:
        print(e)
