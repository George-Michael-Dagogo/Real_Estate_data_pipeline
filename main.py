from bs4 import BeautifulSoup
import requests
import pandas as pd
import re


url = [f'https://www.propertypro.ng/property-for-short-let?sort=beds&order=desc&page={i:d}'  for i in (range(0, 67))]


titles= []
types = []
locations = []
prices = []
date_posted = []
PIDs = []
furnished = []
beds = []


def extract_data(url):
    print('began')
    page = requests.get(url)
    soup = BeautifulSoup(page.text,  "html.parser")
    house_box = soup.find_all('div', class_ = "single-room-sale listings-property")
    for house in house_box:
#titles
        if house.find('h3', class_ = "listings-property-title2") is not None:
            title = house.find('h3', class_ = "listings-property-title2").text
            titles.append(title)
        else:
            titles.append('No title')

#types
        if house.find('h4', class_ = "listings-property-title") is not None:
            type = house.find('h4', class_ = "listings-property-title").text
            types.append(type)
        else:
            types.append('No type')

#locations
        if house.find('h4') is not None:
            locate = house.find_all('h4')
            location = locate[1].text
            locations.append(location)
        else:
            locations.append('No location')

#prices
        if house.find('h3', class_ = "listings-price") is not None:
            price = house.find('h3', class_ = "listings-price").text
            prices.append(price)
        else:
            prices.append('No price')

#date_posted
        if house.find('h5') is not None:
            date = house.find('h5').text
            date_posted.append(date)
        else:
            date_posted.append('No date')

#PIDs
        if house.find('h2') is not None:
            PID = house.find('h2').text.replace('PID:','')
            PIDs.append(PID)
        else:
            PIDs.append('No PID')

#furnished, serviced, newly built
        if house.find('div', class_ = "furnished-btn") is not None:
            furnish = house.find('div', class_ = "furnished-btn").text
            furnished.append(furnish)
        else:
            furnished.append('0')

#utilities
        if house.find('div', class_ = "fur-areea") is not None:
            bed = house.find('div', class_= "fur-areea").text.replace('\n',' ').strip()
            beds.append(bed)
        else:
            beds.append('No beds')


def transform_data():
    df = pd.DataFrame({'title': titles, 
                            'categories': types,
                            'address': locations,
                            'price': prices,
                            'date_posted': date_posted,
                            'PIDs': PIDs,
                            'furnish': furnished,
                            'bed': beds})

    df['newly_built'] = df['furnish'].apply(lambda text: 'Newly Built' in text)
    df['serviced'] = df['furnish'].apply(lambda text: 'Serviced' in text)
    df['furnished'] = df['furnish'].apply(lambda text: 'Furnished' in text)
    df.drop('furnish', axis=1, inplace=True)
    df[['beds', 'baths', 'toilets']] = df['bed'].str.extract(r'(\d+)\s*beds?\s*(\d*)\s*baths?\s*(\d*)\s*Toilets?')
    df['beds'] = df['beds'].replace('', 0).astype(int)
    df['baths'] = df['baths'].replace('', 0).astype(int)
    df['toilets'] = df['toilets'].replace('', 0).astype(int)
    df['price'] = df['price'].str.replace('₦', '')

    df['price_int'] = pd.to_numeric(df['price'].str.replace(',', '').str.extract(r'(\d+)')[0])

    df['price_per_day_₦'] = df['price_per_month_₦'] = df['price_per_year_₦'] = pd.NA

    for index, row in df.iterrows():
        if 'day' in row['price']:
            df.at[index, 'price_per_month_₦'] = row['price_int'] * 30
            df.at[index, 'price_per_year_₦'] = row['price_int'] * 365
            df.at[index, 'price_per_day_₦'] = row['price_int']
        elif 'month' in row['price']:
            df.at[index, 'price_per_day_₦'] = row['price_int'] / 30
            df.at[index, 'price_per_year_₦'] = row['price_int'] * 12
            df.at[index, 'price_per_month_₦'] = row['price_int']
        elif 'year' in row['price']:
            df.at[index, 'price_per_month_₦'] = row['price_int'] / 12
            df.at[index, 'price_per_day_₦'] = row['price_int'] / 365
            df.at[index, 'price_per_year_₦'] = row['price_int']
        else:
            df.at[index, 'price_per_year_₦'] = row['price_int'] * 365
            df.at[index, 'price_per_month_₦'] = row['price_int'] * 12
            df.at[index, 'price_per_day_₦'] = row['price_int']

    df.drop('price_int', axis=1, inplace=True)

    df['date_posted'] = df['date_posted'].str.extract(r'Added (\d{2} \w{3} \d{4})', expand=False)
    df['date_posted'] = pd.to_datetime(df['date_posted'], format='%d %b %Y')
    df['state'] = df['address'].str.split().str[-1]
    df.to_csv('propertypro_short_let.csv', index=False)

for i in url:
    print(i)
    extract_data(i)

transform_data()