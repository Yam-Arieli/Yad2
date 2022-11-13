import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from headers import HEADERS

class Product:
    """
    Easy to use
    Error free
    Correct names of products for use as arg
    """
    houses = 'realestate'
    cars = 'vehicles/cars'

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

def get_car_data(car) -> dict:
    brand = car.find('span', class_='title').text.strip()
    brand2 = car.find('span', class_='subtitle').text.strip()
    price = car.find('div', class_='price').text.strip()
    year = car.find('div', class_='data year-item').find('span', class_='val').text

    try:
        hand = car.find('div', class_='data hand-item').find('span', class_='val').text
    except:
        hand = 1
    try:
        engine_size = car.find('div', class_='data engine_size-item').find('span', class_='val').text
    except:
        engine_size = None
    
    
    # additional
    brac1 = brand2.find('(')
    brac2 = brand2.find(')')
    hourse_power = brand2[brac1+1 : brac2].replace("כ''ס", '').replace('כ"ס', '')
    brand2 = brand2[:brac1] + brand2[brac2+1:]
#     fuel = 
#     seats = 
    automatic_trans = brand2.find("ידני") < 0
    type_ = 'unknown'
    for t in ('בנזין', 'סולר', 'חשמלי', 'דיזל', 'הייבריד'):
        if t in brand2:
            type_ = t
            brand2 = brand2.replace(t, '')
            break
    
    brand2 = brand2.replace("אוט' ", '') if automatic_trans else brand2.replace("ידני ", '')
    
    result = {
        'brand': brand,
        'brand2': brand2,
        'price': price,
        'year': year,
        'hand': hand,
        'engine_size': engine_size,
        'automatic_trans': automatic_trans,
        'hourse_power': hourse_power,
        'type': type_
    }
    return result


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

def get_data_function_by_name(product: str) -> callable:
    match product:
        case 'realestate':
            return get_house_data
        
        case 'vehicles/cars':
            return get_car_data
        
        case _:
            raise Exception(f'Unknown product name: {product}')

def feeditems_to_data_pre_normal(feeditems: List[BeautifulSoup], get_data_function: callable) -> List[dict]:
    return [get_data_function(feeditem) for feeditem in feeditems]

def get_posts_data_as_list_of_dicts(last_page_num=10, product='realestate', max_workers=64) -> List[dict]:
    result = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        func = get_data_function_by_name(product)
        res_list = executor.map(
            lambda i: feeditems_to_data_pre_normal(get_feeditems(i, product), func),
            range(1, last_page_num)
        )
        for res in res_list:
            result.extend(res)
    
    # Posts with no numeric price return empty dict and need to be removed
    # filter out the empty dicts
    return list(filter(lambda x: x, result))

def get_posts_DataFrame(last_page_num=10, product='realestate') -> pd.DataFrame:
    # TODO: Check if product is valid
    return pd.json_normalize(get_posts_data_as_list_of_dicts(last_page_num, product))