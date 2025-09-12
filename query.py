#!/usr/bin/env python3
import argparse
from metadata_products import Metadata_products
from utils import load_values_from_config, init_logging, get_dict_satellites_and_product_types, date_from_string
import sys
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import json

def parse_args():
    parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
            description='Script to downloaded products from CDSE between two given dates. Inputs are: \n'+
            '--start_date: first date of query, \n' +
            '--end_date: last date of query, \n' +
            '--sat: satelite to query data from (if not provided downloading from all sentinel 2 satelites)' 
            )
    
    parser.add_argument("--start_date", type=str, required=True, help="First date you want to download products for (yyyymmdd)")
    parser.add_argument("--end_date", type=str, required=True, help="First date you want to download products for (yyyymmdd)")
    parser.add_argument("--sat", type=str, help="For which satellite do you want to harvest products?", choices=valid_satellites)

    return parser.parse_args()


(
    username,
    password,
    output_dir,
    polygon_wkt,
    valid_satellites,
    polygon,
    product_types_csv
) = load_values_from_config()

# Log to console
logger = init_logging()

def main(start_date, end_date, satellites):

    dates = []
    current = date_from_string(start_date)
    while current <= date_from_string(end_date):
        dates.append(current.strftime("%Y%m%d"))
        current += relativedelta(days=1)

    S2A = {}
    S2B = {}
    S2C = {}

    for i in range(0, len(dates)-1):

        for sat in satellites:
            if sat not in valid_satellites:
                print(sat)
                logger.info(f"------Invalid 'sat' value. Valid values are: {', '.join(valid_satellites)}------")
                sys.exit(1)

            satellites_and_product_types = get_dict_satellites_and_product_types(sat)

            for satellite, productTypes in satellites_and_product_types.items():
                for productType in productTypes:
                    metadata_products = Metadata_products(satellite, productType, dates[i], dates[i+1])
                    S2A_cloud_coverage, S2B_cloud_coverage, S2C_cloud_coverage = metadata_products.harvest_all_products_to_json()

                    for record in S2A_cloud_coverage:
                        for product, cc in record.items():
                            year = product.split('_')[2][0:4]
                            if not year in S2A:
                                S2A[year] = {}
                            S2A[year][product] = cc

                    for record in S2B_cloud_coverage:
                        for product, cc in record.items():
                            year = product.split('_')[2][0:4]
                            if not year in S2B:
                                S2B[year] = {}
                            S2B[year][product] = cc

                    for record in S2C_cloud_coverage:
                        for product, cc in record.items():
                            year = product.split('_')[2][0:4]
                            if not year in S2C:
                                S2C[year] = {}
                            S2C[year][product] = cc


    for year in S2A.keys():

        filepath = f'{output_dir}/S2A_{year}_cc.json'
        os.makedirs(os.path.dirname(filepath), exist_ok=True)


        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(S2A[year], f, ensure_ascii=False, indent=4)
            logger.info(f"------File created: {filepath}-------")

    for year in S2B.keys():
        filepath = f'{output_dir}/S2B_{year}_cc.json'
        os.makedirs(os.path.dirname(filepath), exist_ok=True)


        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(S2B[year], f, ensure_ascii=False, indent=4)
            logger.info(f"------File created: {filepath}-------")

    for year in S2C.keys():
        filepath = f'{output_dir}/S2C_{year}_cc.json'
        os.makedirs(os.path.dirname(filepath), exist_ok=True)


        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(S2C[year], f, ensure_ascii=False, indent=4)
            logger.info(f"------File created: {filepath}-------")



if __name__ == "__main__":

    args = parse_args()

    if not args.sat:
        main(args.start_date, args.end_date, ['S2L2A', 'S2L1C'])
    else:
        main(args.start_date, args.end_date, [args.sat])
       

