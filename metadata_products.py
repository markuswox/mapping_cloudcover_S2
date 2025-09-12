import requests
import json
import os
from utils import date_from_string, load_values_from_config, init_logging
import sys

(
    username,
    password,
    output_dir,
    polygon_wkt,
    valid_satellites,
    polygon,
    product_types_csv
) = load_values_from_config(config_file='./config.yaml')

logger = init_logging()

class Metadata_products:

    def __init__(self, satellite=None, productType=None, start_date=None, end_date=None, json_filepath=None):
        if satellite:
            self.satellite = satellite
        if productType:
            self.productType = productType
        if start_date:
            self.start_date = date_from_string(start_date)
        if end_date:
            self.end_date = date_from_string(end_date)

        if json_filepath:
            self.filepath = json_filepath
        elif self.satellite and self.productType and self.start_date and self.end_date:
            self.filename = f'CDSE_{satellite}_{productType}_{start_date}_cloud_coverage.json'
            #self.create_storage_paths()
            self.filepath = os.path.join(output_dir, self.filename)
        else:
            logger.info(f"------Class requires you to provide either a JSON file to download products or all 4 arguements (satellite, productType, start_date, end_date) ")
            sys.exit(1)

    def harvest_all_products_to_json(self):
        logger.info(f"------Creating JSON file of {self.satellite} {self.productType} products that are present on CDSE between {self.start_date} and {self.end_date} -------")
        base_url = "https://catalogue.dataspace.copernicus.eu/resto/api/collections/"
        maxRecords = 2000 # Number of products to query in one go

        # Initialize an empty list to store the records
        self.all_records = []

        page = 1

        while True:

            # Create the URL with the current offset and limit
            if self.productType == 'all':
                url = f"{base_url}{self.satellite}/search.json?startDate={self.start_date}T00:00:00Z&completionDate={self.end_date}T00:00:00Z&sortParam=startDate&geometry={polygon}&maxRecords={maxRecords}&page={page}"
            else:
                url = f"{base_url}{self.satellite}/search.json?productType={self.productType}&startDate={self.start_date}T00:00:00Z&completionDate={self.end_date}T00:00:00Z&sortParam=startDate&geometry={polygon}&maxRecords={maxRecords}&page={page}"
            # Make the request and get the JSON response
            response = requests.get(url).json()


            # Append the records from the current response to the list
            self.all_records.extend(response.get("features", []))

            # # Append the records from the current response to the list
            # self.all_records.extend(find_key(response, "cloudCover"))#response.get("cloudCover", []))

            # Check if there are more records to fetch
            if len(response.get("features", [])) < maxRecords:
                break  # No more records to fetch

            # Increment page for the next request
            page = page + 1

        S2A_cloud_coverage = [

            {   
                feature.get("properties", {}).get("title")[:-5]: feature.get("properties", {}).get("cloudCover")
            }

            for feature in self.all_records if feature.get('properties').get('title').split('_')[0] == 'S2A'
        ]

        S2B_cloud_coverage = [

            {   
                feature.get("properties", {}).get("title")[:-5]: feature.get("properties", {}).get("cloudCover")
            }

            for feature in self.all_records if feature.get('properties').get('title').split('_')[0] == 'S2B'
        ]

        S2C_cloud_coverage = [

            {   
                feature.get("properties", {}).get("title")[:-5]: feature.get("properties", {}).get("cloudCover")
            }

            for feature in self.all_records if feature.get('properties').get('title').split('_')[0] == 'S2C'
        ]

        
        return S2A_cloud_coverage, S2B_cloud_coverage, S2C_cloud_coverage
        




