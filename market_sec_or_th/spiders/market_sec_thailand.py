import subprocess

from scrapy.cmdline import execute
from lxml.html import fromstring
from datetime import datetime
from typing import Iterable
from scrapy import Request
import pandas as pd
import unicodedata
import random
import string
import scrapy
import json
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
        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column
        data_frame[column] = data_frame[column].apply(set_na)  # Setting "N/A" where data is "No Information" or some empty string characters on site
        if 'date' in column:
            data_frame[column] = data_frame[column].apply(set_date_format)  # Set the Date format
        if 'name' in column:
            data_frame[column] = data_frame[column].apply(remove_diacritics)  # Remove diacritics characters
            data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column
            """Cannot remove punctuations from 'name' header because website urls are also coming in 'name'"""
            data_frame[column] = data_frame[column].apply(remove_punctuation)  # Remove extra spaces and newline characters from each column
        if 'alias' in column:
            data_frame[column] = data_frame[column].apply(remove_diacritics)  # Remove diacritics characters
            data_frame[column] = data_frame[column].apply(remove_punctuation)  # Removing Punctuation from name text
        data_frame[column] = data_frame[column].apply(remove_extra_spaces)  # Remove extra spaces and newline characters from each column
    data_frame.replace(to_replace='nan', value=pd.NA, inplace=True)  # After cleaning, replace 'nan' strings back with actual NaN values
    data_frame.fillna(value='N/A', inplace=True)  # Replace NaN values with "N/A"
    print('DataFrame Cleaned...!')
    return data_frame


def remove_diacritics(input_str):
    return input_str if input_str == 'N/A' else ''.join(char for char in unicodedata.normalize('NFD', input_str) if not unicodedata.combining(char))


# # Function to remove all punctuation
# def remove_punctuation(text):
#     return text if text == 'N/A' else ''.join(char for char in text if not unicodedata.category(char).startswith('P'))


# Function to remove all punctuation
import re
import unicodedata


def remove_punctuation(text):
    if text == 'N/A':
        return text

    # Regex pattern to match URLs (http, https, or www) and exclude trailing punctuation
    url_pattern = r'(https?://[^\s,]+|www\.[^\s,]+)'

    # Find all URLs in the text
    urls = re.findall(url_pattern, text)

    # Replace URLs with placeholders
    placeholder = "siteURL"
    text_with_placeholders = re.sub(url_pattern, placeholder, text)

    # Remove punctuation from the non-URL parts
    text_without_punctuation = ''.join(
        char for char in text_with_placeholders if not unicodedata.category(char).startswith('P')
    )

    # Restore the URLs from placeholders
    for url in urls:
        text_without_punctuation = text_without_punctuation.replace(placeholder, url, 1)

    return text_without_punctuation


def set_na(text: str) -> str:
    # Remove extra spaces (assuming remove_extra_spaces is a custom function)
    text = remove_extra_spaces(text=text)
    # pattern = r'^(Sem Informação|\*{1,}|\.{1,}|\(Não Informado\))$'  # Define a regex pattern to match all the conditions in a single expression
    pattern = r'^(sem Informação|Sem informação|sem informação|Sem Informação|\(Não Informado\)|[^\w\s]+)$'  # Define a regex pattern to match all the conditions in a single expression
    text = re.sub(pattern=pattern, repl='N/A', string=text)  # Replace matches with "N/A" using re.sub
    return text


def set_date_format(text: str) -> str:
    date_pattern = r'(\d{2}/\d{2}/\d{4})'  # Regular expression to extract the date
    match = re.search(date_pattern, text)  # Search for the pattern anywhere in the string
    # If a match is found, try to format the date
    if match:
        date_str = match.group(1)  # Extract the date part from the match
        try:
            date_object = datetime.strptime(date_str, "%d/%m/%Y")  # Try converting the extracted date to a datetime object
            return date_object.strftime("%Y-%m-%d")  # Format the date object to 'YYYY/MM/DD' & return Date string
        except ValueError:
            return text  # If the date is invalid, return the original text
    else:
        return text  # Return the original text if no date is found


# Function to remove Extra Spaces from Text
def remove_extra_spaces(text: str) -> str:
    return re.sub(pattern=r'\s+', repl=' ', string=text).strip()  # Regular expression to replace multiple spaces and newlines with a single space


def header_cleaner(header_text: str) -> str:
    header = remove_diacritics('_'.join(header_text.strip().lower().split()))
    return header


def format_multiple_values(value):
    """Formats multiple values into a single string separated by ' | '."""
    # Split on common delimiters (commas, "หรือ", numbered lists like '1.', '2.')
    # if "หรือ" in value or ',' in value:
    #     parts = value.replace("หรือ", ",").split(',')
    if "1." in value:
        parts = value.split("1.")
        parts = [f"1.{p.strip()}" if p.strip().startswith("Line") else p.strip() for p in parts]
    else:
        parts = [value]

    # Clean and join parts
    return " | ".join(part.strip() for part in parts if part.strip())


class MarketSecThailandSpider(scrapy.Spider):
    name = "market_sec_thailand"

    def __init__(self, *args, **kwargs):
        self.start = time.time()
        super().__init__(*args, **kwargs)
        print('Connecting to VPN (THAILAND)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (THAILAND)
        self.api.connect(country_id='101')  # THAILAND country code for vpn
        time.sleep(5)  # keep some time delay before starting scraping because connecting
        print('VPN Connected!' if self.api.is_connected else 'VPN Not Connected!')

        self.final_data_list = list()  # List of data to make DataFrame then Excel

        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename_native = fr"{self.excel_path}/{self.name}_native.xlsx"  # Filename with Scrape Date
        self.filename_translated = fr"{self.excel_path}/{self.name}_translated.xlsx"  # Filename with Scrape Date

    def start_requests(self) -> Iterable[Request]:
        cookies = {
            '_ga': 'GA1.1.229095061.1732862542',
            '_ga_3NH0QL72D6': 'GS1.1.1733120223.3.1.1733120691.60.0.0',
            'TS01a5e5b3': '012c1f76db794635d8928b4bed6c62e2737430d91cbde238be709c3679235b4b7e524e427486db100edcf6f10a7c78c9d5b6cbc1d0',
            'TS3e96895c027': '08f2067569ab2000818574a54be32af62491801e4bd5ea13c6c1fe40c4af628e9e0f05e24bcbf4f1083f91e1d5113000d8b82ad3222bbb73dfa041195343ff8b36cff81ac6c60a67b5ff6ae4ba850a1bd25c23377a8c59db8280421a812b5dc1',
        }

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Referer': 'https://market.sec.or.th/public/idisc/en/InvestorAlert',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        browsers = ["chrome110", "edge99", "safari15_5"]

        url = 'https://market.sec.or.th/public/idisc/en/Viewmore/invalert-head?PublicFlag=Y'
        yield scrapy.Request(url=url, cookies=cookies, headers=headers, method='GET',
                             meta={'impersonate': random.choice(browsers)}, callback=self.parse, dont_filter=True)

    def parse(self, response, **kwargs):
        parsed_tree = fromstring(response.text)  # Parse the HTML

        headers_list = parsed_tree.xpath("//table//tr/th/text()")  # Extract headers
        rows_list = parsed_tree.xpath("//table//tr[td]")  # Extract rows
        print(f'No. of data: {len(rows_list)}')

        # Dynamic data extraction
        for row in rows_list:
            values = row.xpath("./td")  # Extract all <td> in the row

            data_dict = dict()
            data_dict['url'] = 'https://market.sec.or.th/public/idisc/en/Viewmore/invalert-head?PublicFlag=Y'

            for index, value in enumerate(values):
                header = header_cleaner(headers_list[index])
                if header == "link":  # Special handling for modal content
                    modal_body = value.xpath(".//div[contains(@class, 'modal-body')]")
                    if modal_body:
                        modal_details = modal_body[0].xpath(".//li")  # Extract all modal details dynamically
                        for detail in modal_details:
                            key_value = ' '.join(detail.xpath('.//text()')).split(sep=": ", maxsplit=1)
                            if len(key_value) > 1:
                                field_name = header_cleaner(key_value[0].strip())
                                # field_value = format_multiple_values(key_value[1].strip())  # Handle multiple values by separating with ` | `
                                field_value = key_value[1].strip()  # Handle multiple values by separating with ` | `
                                if 'website' in field_name:
                                    field_value = ' | '.join(field_value.strip(',').strip().split(','))
                                data_dict[field_name] = field_value

                        # Extract image URL
                        image_url = modal_body[0].xpath(".//img/@src")
                        data_dict["image_url"] = image_url[0] if image_url else "N/A"
                else:
                    value = ' | '.join(value.xpath('.//text()')).strip()
                    if header.lower() == "name":
                        # Extract alias and clean the 'name' using regex
                        # alias_pattern = r"(impersonates?|impersonate)\s*(.+?)(?:$)"  # Regex for finding alias strings after 'impersonates' or 'impersonate'
                        # aliases = re.findall(alias_pattern, value)
                        # alias_values = " | ".join(alias[1].strip() for alias in aliases)

                        # Updated regex to handle impersonates followed by multi-line alias
                        alias_pattern = r"(impersonates?|impersonate)\s*(.+?)(?:$)"  # Regex for finding alias strings after 'impersonates' or 'impersonate'
                        aliases = re.findall(alias_pattern, value, flags=re.DOTALL)  # Use re.DOTALL to allow '.' to match newlines
                        alias_values = " | ".join(alias[1].strip().strip('“”') for alias in aliases)  # Strip and join all alias values found (remove leading/trailing whitespace, smart quotes)
                        data_dict["alias"] = alias_values if alias_values else "N/A"
                        value = re.sub(pattern=alias_pattern, repl="", string=value).strip()  # Remove alias-related text from 'name'

                    data_dict[header] = value if value != '' else 'N/A'

            # print(data_dict)
            self.final_data_list.append(data_dict)

        print('+' * 100)

    def close(self, reason):
        print('closing spider...')
        if self.final_data_list:
            try:
                print("Creating Native sheet...")
                native_data_df = pd.DataFrame(self.final_data_list)
                native_data_df = df_cleaner(data_frame=native_data_df)  # Apply the function to all columns for Cleaning
                native_data_df.insert(loc=0, column='id', value=range(1, len(native_data_df) + 1))  # Add 'id' column at position 1
                with pd.ExcelWriter(path=self.filename_native, engine='xlsxwriter', engine_kwargs={"options": {'strings_to_urls': False}}) as writer:
                    native_data_df.to_excel(excel_writer=writer, index=False)
                print("Native Excel file Successfully created.")
            except Exception as e:
                print('Error while Generating Native Excel file:', e)

            # Run the translation script with filenames passed as arguments
            try:
                # Define the filenames as arguments with source language code
                subprocess.run(
                    args=["python", "translate_and_save.py", self.filename_native, self.filename_translated, 'th'],
                    check=True
                )
                print("Translation completed successfully.")
            except subprocess.CalledProcessError as e:
                print(f"Error during translation: {e}")
        else:
            print('Final-Data-List is empty.')
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()
        end = time.time()
        print(f'Scraping done in {end - self.start} seconds.')


if __name__ == '__main__':
    execute(f'scrapy crawl {MarketSecThailandSpider.name}'.split())
