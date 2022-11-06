import requests

def get_Cookie() -> str:
    with requests.Session() as s:
        res = s.get('https://gw.yad2.co.il/auth/token/refresh')
        co_dict = res.cookies.get_dict()
        
        Cookie = ''
        for key in co_dict:
            Cookie += f'{key}={co_dict[key]}; '
    
    return Cookie

def get_headers() -> dict:
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Host': 'www.yad2.co.il',
        'mobile-app': 'false',
        'Referer': 'https://www.yad2.co.il/realestate/forsale',
        'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
        'sec-ch-ua-platform': '"Windows"',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
    }
    
    headers['Cookie'] = get_Cookie()
    
    return headers

HEADERS = get_headers()
print('Created HEADERS:')
print(HEADERS)