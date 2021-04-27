import time
import csv
import random
import os
from datetime import date
from itertools import groupby

import requests
from bs4 import BeautifulSoup

per_page = 25
html_dir = 'pages'
outfilename = 'in-salary-data.csv'

user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'  # noqa

today = date.today().isoformat()

csv_headers = [
    'first',
    'last',
    'dept',
    'status',
    'salary',
    'access_date'
]

headers = {
    'user-agent': user_agent
}

params = {
    'searchPerformed': True,
    'firstName': '',
    'lastName': '',
    'agency': '',
    'offset': 1
}

base_url = 'https://www.in.gov/apps/gov/salaries/'


# https://stackoverflow.com/a/55317300
def uniq(lst):
    for _, grp in groupby(lst, lambda d: (d['first'], d['last'], d['dept'], d['status'], d['salary'])):  # noqa
        yield list(grp)[0]


def get_page_limit():
    r = requests.get(
        base_url,
        headers=headers,
        params=params
    )
    soup = BeautifulSoup(r.text, 'html.parser')
    return int(soup.find_all('a', {'class': 'step'})[-1].text)


def download_pages():
    num_pages = get_page_limit()
    largest_offset = num_pages * per_page
    while params['offset'] < largest_offset:
        r = requests.get(
            base_url,
            headers=headers,
            params=params
        )
        filename = ''.join([
            str(params['offset']).zfill(5),
            '_',
            str(params['offset'] + per_page).zfill(5),
            '.html'
        ])
        filepath = os.path.join(html_dir, filename)

        with open(filepath, 'w') as outfile:
            outfile.write(r.text)

        print(f'Wrote {filepath}')

        params['offset'] = params['offset'] + per_page
        time.sleep(random.uniform(1, 3))


def scrape_pages():
    data = []
    files = [os.path.join(html_dir, x) for x in os.listdir(html_dir) if x.endswith('.html')]  # noqa
    with open(outfilename, 'w') as outfile:
        writer = csv.DictWriter(
            outfile,
            fieldnames=csv_headers
        )

        writer.writeheader()

        for f in files:
            with open(f, 'r') as infile:
                html = infile.read()
                soup = BeautifulSoup(html, 'html.parser')
                table = soup.find_all('table')[-1]
                rows = table.find_all('tr')

                for row in rows[1:]:
                    if 'Your search criteria' in row.text:
                        continue
                    first, last, dept, status, salary = row.find_all('td')
                    d = {
                        'first': first.text.strip(),
                        'last': last.text.strip(),
                        'dept': dept.text.strip(),
                        'status': status.text.strip(),
                        'salary': salary.text.strip().replace('$', '').replace(',', ''),  # noqa
                        'access_date': today
                    }
                    data.append(d)
        data_sorted = sorted(
            data,
            key=lambda d: (d['first'], d['last'], d['dept'], d['status'], d['salary']) # noqa
        )
        writer.writerows(list(uniq(data_sorted)))


scrape_pages()
