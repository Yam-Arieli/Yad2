import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from headers import HEADERS

def get_feeditems(page_num=1, product='realestate') -> List[BeautifulSoup]:
    if page_num % 50 == 0:
        print(f'{page_num} pages has been scaned so far')
    
    uri = f'https://www.yad2.co.il/{product}/forsale?page={page_num}'

    res = requests.get(uri, headers=HEADERS)
    soup = BeautifulSoup(res.text, "html.parser")
    
    return soup.find_all('div', class_="feeditem table")

def get_item_date(item: BeautifulSoup) -> str:
    if (today := item.find('span', class_='date')) and ((today := today.string.strip()) != 'עודכן היום'):
        return today
    else:
        return datetime.today().strftime('%Y/%m/%d')

def get_house_data(feeditem: BeautifulSoup) -> dict:
    title = feeditem.find('span', class_='title').text.strip()

    subtitle = feeditem.find('span', class_='subtitle').text
    subtitle = tuple(sub_part.strip() for sub_part in subtitle.split(','))
    type_ = subtitle[0]
    city = subtitle[-1]

    price = feeditem.find('div', class_='price').string.strip()

    attr = tuple(span.string for span in feeditem.find('div', class_="middle_col").find_all('span'))
    rooms, floor, size = str(attr[0]), str(attr[2]), str(attr[4])

    try:
        price = int(price[:-2].replace(',', ''))
        return {
            'price': price,
            'address': title,
            'city': city,
            'type': type_,
            'rooms': int(rooms),
            'floor': int(floor),
            'size': int(size),
            'update': get_item_date(feeditem),
            'scrap_date': datetime.today().strftime('%Y/%m/%d')
        }
    except:
        return {}

def feeditems_to_data_pre_normal(feeditems: List[BeautifulSoup]) -> List[dict]:
    return [get_house_data(feeditem) for feeditem in feeditems]

def get_posts_data_as_list_of_dicts(last_page_num=10, product='realestate') -> List[dict]:
    result = []

    with ThreadPoolExecutor(max_workers=64) as executor:
        res_list = executor.map(lambda i: feeditems_to_data_pre_normal(get_feeditems(i, product)), range(1, last_page_num))
        for res in res_list:
            result.extend(res)
    
    # Posts with no numeric price return empty dict and need to be removed
    # filter out the empty dicts
    return list(filter(lambda x: x, result))

def get_posts_DataFrame(last_page_num=10, product='realestate') -> pd.DataFrame:
    # TODO: Check if product is valid
    return pd.json_normalize(get_posts_data_as_list_of_dicts(last_page_num, product))
    