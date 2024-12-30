from scrapy.cmdline import execute
from lxml.html import fromstring
from unidecode import unidecode
from datetime import datetime
from typing import Iterable
from scrapy import Request
from urllib import parse
import pandas as pd
import random
import string
import scrapy
import time
import evpn
import os
import re


def df_cleaner(data_frame: pd.DataFrame) -> pd.DataFrame:
    print('Cleaning DataFrame...')
    data_frame = data_frame.astype(str)  # Convert all data to string
    data_frame.drop_duplicates(inplace=True)  # Remove duplicate data from DataFrame

    # Apply the function to all columns for Cleaning
    for column in data_frame.columns:
        data_frame[column] = data_frame[column].apply(set_na)  # Setting "N/A" where data is "No Information" or some empty string characters on site
        data_frame[column] = data_frame[column].apply(unidecode)  # Remove diacritics characters
        # data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column
        if 'name' in column:
            data_frame[column] = data_frame[column].str.replace('–', '')  # Remove specific punctuation 'dash' from name string
            data_frame[column] = data_frame[column].str.translate(str.maketrans('', '', string.punctuation))  # Removing Punctuation from name text
        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column

    data_frame.replace(to_replace='nan', value=pd.NA, inplace=True)  # After cleaning, replace 'nan' strings back with actual NaN values
    data_frame.fillna(value='N/A', inplace=True)  # Replace NaN values with "N/A"
    print('DataFrame Cleaned...!')
    return data_frame


def set_na(text: str) -> str:
    # Remove extra spaces (assuming remove_extra_spaces is a custom function)
    text = remove_extra_spaces(text=text)
    pattern = r'^([^\w\s]+)$'  # Define a regex pattern to match all the conditions in a single expression
    text = re.sub(pattern=pattern, repl='N/A', string=text)  # Replace matches with "N/A" using re.sub
    return text


# Function to remove Extra Spaces from Text
def remove_extra_spaces(text: str) -> str:
    return re.sub(pattern=r'\s+', repl=' ', string=text).strip()  # Regular expression to replace multiple spaces and newlines with a single space


def header_cleaner(header_text: str) -> str:
    header_text = header_text.strip()
    header = unidecode('_'.join(header_text.lower().split()))
    return header


def get_image_url(main_page) -> str:
    image_url: str = ' | '.join(main_page.xpath('//img[@class="person-photo"]/@src'))
    return image_url if image_url not in ['-', '', ' '] else 'N/A'


def get_full_name(main_page) -> str:
    name: str = ' | '.join(main_page.xpath('./h2/text()'))
    return name if name not in ['-', '', ' '] else 'N/A'


def get_value(person_info_div, class_value):
    value = ' '.join(person_info_div.xpath(f'./div[@class="{class_value}"]/text()'))
    # Replace newline characters and multiple spaces with a single space, and strip leading/trailing spaces
    value = re.sub(pattern=r'\s+', repl=' ', string=value).strip()
    return value if value not in ['-', '', ' '] else 'N/A'


def extract_phone_numbers(text):
    """
    Extract phone numbers from a given text using regex.
    Supports various formats like (XXX) XXX-XXXX, XXX-XXX-XXXX, etc.
    """
    # phone_pattern = re.compile(pattern=r'\(?\d{3,4}\)?[-.\s]?\d{2,3}[-.\s]?\d{2,3}[-.\s]?\d{2,3}')
    phone_pattern = re.compile(pattern=r'\(?\d{3,4}\)?[-.\s]?\d{,5}[-.\s]?\d{2,3}[-.\s]?\d{2,3}')
    phone_numbers = phone_pattern.findall(text)
    return [phone_number.replace(')', '').replace('(', '') for phone_number in phone_numbers]


# Function to extract alias from full name
def extract_alias(full_name):
    # Regex to capture text inside parentheses
    alias_pattern = re.compile(r'\((.*?)\)')
    match = alias_pattern.search(full_name)
    alias = match.group(1) if match else 'N/A'
    # Remove the alias part (including parentheses) from the full name
    updated_full_name = alias_pattern.sub(repl='', string=full_name).strip()
    return updated_full_name, alias


# Function Convert a list of dates from 'DD Month YYYY' format to 'YYYY/MM/DD' format.
def convert_date_format(date_str):
    if date_str != 'N/A':
        date_obj = datetime.strptime(date_str, "%d %B %Y")  # Parse the date using datetime.strptime
        formatted_date = date_obj.strftime(format="%Y-%m-%d")  # Convert to desired format
        return formatted_date
    else:
        return date_str


class SsuGovUkraineSpider(scrapy.Spider):
    name = "ssu_gov_ukraine"

    def __init__(self, *args, **kwargs):
        self.start = time.time()
        super().__init__(*args, **kwargs)
        print('Connecting to VPN (UKRAINE)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (UKRAINE)
        self.api.connect(country_id='87')  # UKRAINE country code for vpn
        time.sleep(5)  # keep some time delay before starting scraping because connecting
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        # self.delivery_date = datetime.now().strftime('%Y%m%d')
        self.final_data_list = list()  # List of data to make DataFrame then Excel

        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename = fr"{self.excel_path}/{self.name}.xlsx"  # Filename with Scrape Date

        self.cookies = {
            'ak_bmsc': '57A068887D7D43AE8AB82F420C555CE4~000000000000000000000000000000~YAAQvf3UF8X3ZGSTAQAAi3aphhm6Dr5+BJ8I1IfrcnK2TyBUYq0aVD6qYCdEK+NAYY6/7FWYBKnEXoz6q1ppwakqS1U4ApNZQFLTYfLQu4J4v6SpcPL87VjuW+NQ4DiMBFgXVV1AiumZceaokS1a0U19Wf6AFppcYYvtjiNJL734WqNPuKgpJD16dNVr7LFEsuAKpNiz/h+sRXJFmqOGy/MUBgCT3VSpu4KzXQgx4C2qRvVfYLN0kaIkrBFdsXyAuBAb0GYRzeMWo+GAtq/6sPEumN5px5kO2U+vU5dX196z/HL/GOvsavGSRbELvfaZCCvVPWPJJzKuFWjrwifMgCGS7x2bovNJrDxAZEjtqvI630bP006lnCXDwBDbmzERhoIDigPU21UT1rqQ+tDOLlt1lu3WbUm3SCR/r9ccynPchrCIgPJXRhyI5eiT4BgaCbH/tmynNf3P1G/QYpXTJoJC6sF3Aq57NWnE1gzuCLpkAQjS95cbypeh4qCtqBcsHIsjEq74/ja7d2obpxEvr9IDV4AeZmNxxETzNsjR5r0C1UHo',
            'bm_mi': 'DE970D6914EB8437F33CF5538D94A197~YAAQvf3UF6sDZWSTAQAAKA2rhhk671+yKTU4U5BOC7LUXVoZtHFIbH963CzajvTUqUr4KHDybnv97xA5ny5syOFB9TKfB9WlXefy2gqKvgGw/rCFUBFFg9grhxzoV6/mpsdeESv7D4URCglN2rjd1KRx4za46jrPJZFYvB26WtU2t+vwGheqJdMP36gOeqEuGqvSkr8aCmWvuGMtpho3sct03L5DLck/2YkBNm7hB66XnDKniYo8VEHTljVNb0rYQ/EWE6C1ZryENzVjf4KzkCQUu64vPcydCUUTSfV2m+Nc4BozxyEw4aMxs7pFWNPEdUWMppshldwV~1',
            'XSRF-TOKEN': 'eyJpdiI6IkYwcVF5aW5qSVVRQlFiQ0h0TkhoSnc9PSIsInZhbHVlIjoiYnZzWGNMWFV0WUlWOSt2MkR6SU84eVV3WTBsOHA4MTRMY0piV2htSG15SGVhZzVPMmdLOU5PZ2FPajh4VVwvWklXdzVGM09CU08xWFwvM1ZhcjJoNzJQUT09IiwibWFjIjoiM2EzYzg1MjQ5NGJhNzYwZjNmZmJjMTZmZmIxZDVlOTQ4YmFjMWI2Y2M3ZTBkODg1M2YzMzBkY2U2YTk3Y2RkNSJ9',
            'ssu_session': 'eyJpdiI6IkZzZTNFTm1CWHNiZE8rOUw2VEtjUFE9PSIsInZhbHVlIjoiNlNQUm9va2tuK0xoVjVLdTRJV0p1NkFVV3VJNTc1Y0hOKzNISklZbEViVG1UdWNwaUllbU5LYUIzWHpZT3VTQnhKSUxUOG55WHdFdHNxdGtld3lTa0E9PSIsIm1hYyI6IjA5NGI5Njk0YjQzNmMzNzg5NjUxN2IyNWY0ZDRhMDVmMTFhN2YzYTYzOTdkZWExMWQ3NmY4MDlhZTc3MjIyMjQifQ%3D%3D',
            'bm_sv': 'F77C4D3922CCF1778E9F3073BCC347E7~YAAQv/3UF3+3hi+TAQAAGyOrhhl8WJZBRk7PG+6oxF0f1zXmvERUQOxgfyiJLHQsigJ5TOoyabcWsyzVxLFc/DpdH36/Cui/OQOPU6CE6fgVO58u3wQotkEmMiivS03gzwatYBp/Mv9YDl3LPjNxmT9ymHDSBJWV8hdpVYVPT3CXTz628CARVqI+UQAYwxPPorC9YhxTV8JkPaCMTq/VyIJTKVcH4aZMmqdGokgpJdlsRj6yvfpUdHPOFxD/onWl~1',
        }

        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=0, i',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

    def start_requests(self) -> Iterable[Request]:
        browsers = ["chrome110", "edge99", "safari15_5"]
        params = {'page': '1'}
        url = 'https://ssu.gov.ua/en/u-rozshuku?' + parse.urlencode(params)
        yield scrapy.Request(url=url, cookies=self.cookies, headers=self.headers, method='GET', meta={'impersonate': random.choice(browsers)},
                             callback=self.parse, dont_filter=True, cb_kwargs={'params': params})

    def parse(self, response, **kwargs):
        params = kwargs['params']
        parsed_tree = fromstring(response.text)  # Parse the HTML

        # Extract your data here...
        print("Extracting data from page:", params['page'])

        wanted_list = parsed_tree.xpath('//ul[@class="wanted-list"]/li')
        # Request on details page for each criminal
        for criminal_tag in wanted_list:
            criminal_url = r'https://ssu.gov.ua/' + ''.join(criminal_tag.xpath('./a/@href'))
            yield scrapy.Request(url=criminal_url, cookies=self.cookies, headers=self.headers, method='GET', meta={'impersonate': random.choice(["firefox", "chrome"])},
                                 callback=self.parse_criminal_page, dont_filter=True, cb_kwargs={'page_url': response.url, 'criminal_url': criminal_url})

        # Find the URL of the next page & Handle Pagination
        pagination_links = parsed_tree.xpath("//ol[contains(@class, 'pagination')]//a/@href")
        if pagination_links:
            # Determine the last page link (if it's not a dynamic number, it's the last listed link)
            last_page_url = pagination_links[-1]
            current_page = int(params['page'])

            parsed_url = parse.urlparse(last_page_url)  # Parse the URL
            query_params = parse.parse_qs(parsed_url.query)  # Extract query parameters
            # Get the value of the 'page' parameter
            page = int(query_params.get('page', [None])[0])  # Use [None] as default if 'page' doesn't exist

            # If the last page URL indicates there's more data, make the next request
            if current_page != page + 1:  # compare current page with last page count + 1 as site gives second last page link in pagination buttons
                next_params = {'page': str(current_page + 1)}
                print('Sending request on next page', next_params)
                next_url = 'https://ssu.gov.ua/en/u-rozshuku?' + parse.urlencode(next_params)
                # , meta={'impersonate': random.choice(["firefox", "chrome"])}
                yield scrapy.Request(url=next_url, cookies=self.cookies, headers=self.headers, method='GET',
                                     callback=self.parse, dont_filter=True, cb_kwargs={'params': next_params})
            else:
                print('No More Pagination found.')
        print('+' * 100)

    def parse_criminal_page(self, response, **kwargs):
        parsed_tree = fromstring(response.text)  # Parse the HTML
        main_page = parsed_tree.xpath('//main[@class="wanted-page"]')[0]

        # Scraping Person Information
        person_info_divs = main_page.xpath('./div[@class="person-info"]//div[@class="person-prop"]')
        data_dict: dict = dict()

        full_name = get_full_name(main_page)

        # Extract alias and clean full name
        cleaned_full_name, alias = extract_alias(full_name)
        data_dict['url'] = kwargs['page_url']
        data_dict['criminal_url'] = kwargs['criminal_url']
        data_dict['full_name'] = cleaned_full_name
        data_dict['alias'] = alias  # Add the alias key
        data_dict['image_url'] = get_image_url(main_page)

        for person_info_div in person_info_divs:
            header = get_value(person_info_div, class_value='label')
            value = get_value(person_info_div, class_value='value')
            if 'name' in header.lower():
                value = value.replace(f'({alias})', '')
            elif 'date' in header.lower():
                value = convert_date_format(value)

            header = header_cleaner(header)
            data_dict[header] = value

            if 'contact' in header.lower():
                extracted_phones = extract_phone_numbers(value)  # Extract phone numbers from the value using regex
                if extracted_phones:
                    data_dict['phone'] = " | ".join(extracted_phones) if extracted_phones else "N/A"  # Add phone numbers to data_dict
        self.final_data_list.append(data_dict)

    def close(self, reason):
        print('closing spider...')
        print("Converting List of Dictionaries into DataFrame, then into Excel file...")
        try:
            print("Creating Native sheet...")
            data_df = pd.DataFrame(self.final_data_list)
            data_df = df_cleaner(data_frame=data_df)  # Apply the function to all columns for Cleaning

            data_df.insert(loc=0, column='id', value=range(1, len(data_df) + 1))  # Add 'id' column at position 1
            # data_df.set_index(keys='id', inplace=True)  # Set 'id' as index for the Excel output
            with pd.ExcelWriter(path=self.filename, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
                data_df.to_excel(excel_writer=writer, index=False)

            print("Native Excel file Successfully created.")
        except Exception as e:
            print('Error while Generating Native Excel file:', e)
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()

        end = time.time()
        print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute(f'scrapy crawl {SsuGovUkraineSpider.name}'.split())
