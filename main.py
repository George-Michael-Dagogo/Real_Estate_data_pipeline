from bs4 import BeautifulSoup
import requests
import pandas as pd
import re

url = 'https://www.propertypro.ng/property-for-short-let?sort=postedOn&order=desc'

titles= []
types = []
locations = []
prices = []
date_posted = []
PIDs = []
furnished = []
beds = []


def scraper_per_page(url):
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


scraper_per_page(url)

df = pd.DataFrame({'title': titles, 
                        'categories': types,
                        'locations': locations,
                        'prices': prices,
                        'date_posted ': date_posted,
                        'PIDs': PIDs,
                        'furnished': furnished,
                        'beds': beds})

print(df)