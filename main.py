"""
Main controler for right move web scraping script and data analysis.
"""
import argparse
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()

import pandas as pd
import numpy as np

from rightmove_webscraper.rightmove_webscraper import RightmoveData
import geopy
import geopy.distance
import logging as log
from datetime import datetime

import os.path
from os import path

import schedule
import time


def get_today():
    return int(datetime.today().strftime('%Y%m%d'))


log.basicConfig(filename='scraper.log', encoding='utf-8', level=log.DEBUG)
# define a Handler which writes INFO messages or higher to the sys.stderr
console = log.StreamHandler()
console.setLevel(log.INFO)
# set a format which is simpler for console use
formatter = log.Formatter('%(asctime)s: %(levelname)-8s %(message)s')
# tell the handler to use this format
console.setFormatter(formatter)
# add the handler to the root logger
log.getLogger().addHandler(console)




def get_url(post_code):
    """
    Post codes are insufficent, also need to search by wards / regions
    """
    urls = {
    "BS6": "https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&locationIdentifier=OUTCODE%5E292&insId=1&radius=0.0&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=&displayPropertyType=&maxDaysSinceAdded=&_includeSSTC=on&sortByPriceDescending=&primaryDisplayPropertyType=&secondaryDisplayPropertyType=&oldDisplayPropertyType=&oldPrimaryDisplayPropertyType=&newHome=&auction=false",
    "BS8": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=OUTCODE%5E295&insId=1&numberOfPropertiesPerPage=24&areaSizeUnit=sqft&googleAnalyticsChannel=buying",
    "BS3": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=OUTCODE%5E277&insId=1&numberOfPropertiesPerPage=24&areaSizeUnit=sqft&googleAnalyticsChannel=buying",
    "BS2": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=OUTCODE%5E266&insId=1&numberOfPropertiesPerPage=24&areaSizeUnit=sqft&googleAnalyticsChannel=buying",
    "BS1": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=OUTCODE%5E259&insId=1&numberOfPropertiesPerPage=24&areaSizeUnit=sqft&googleAnalyticsChannel=buying",
    "BS7": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=OUTCODE%5E293&insId=1&numberOfPropertiesPerPage=24&areaSizeUnit=sqft&googleAnalyticsChannel=buying",

    "St Andrews": "https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&locationIdentifier=REGION%5E22795&insId=1&radius=0.0&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=&displayPropertyType=&maxDaysSinceAdded=&_includeSSTC=on&sortByPriceDescending=&primaryDisplayPropertyType=&secondaryDisplayPropertyType=&oldDisplayPropertyType=&oldPrimaryDisplayPropertyType=&newHome=&auction=false",
    "Redland": "https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&locationIdentifier=REGION%5E20676&insId=1&radius=0.0&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=&displayPropertyType=&maxDaysSinceAdded=&_includeSSTC=on&sortByPriceDescending=&primaryDisplayPropertyType=&secondaryDisplayPropertyType=&oldDisplayPropertyType=&oldPrimaryDisplayPropertyType=&newHome=&auction=false",
    "Redland Hist.": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E20676&sortType=1&propertyTypes=&includeSSTC=true&mustHave=&dontShow=retirement&furnishTypes=&keywords=",
    "Cotham+Montpelier": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E7153&propertyTypes=&includeSSTC=false&mustHave=&dontShow=&furnishTypes=&keywords=",
    "Clifton": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E6574&propertyTypes=&includeSSTC=false&mustHave=&dontShow=&furnishTypes=&keywords=",
    "Clifton Hist.": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E6574&sortType=1&propertyTypes=&includeSSTC=true&mustHave=&dontShow=retirement&furnishTypes=&keywords=",
    "Southville": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E22658&propertyTypes=&includeSSTC=false&mustHave=&dontShow=&furnishTypes=&keywords=",
    "Bishopston": "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=REGION%5E3650&maxBedrooms=3&minBedrooms=3&sortType=1&propertyTypes=&mustHave=&dontShow=&furnishTypes=&keywords="


    }
    return urls[post_code]


# url_bs6 = "https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&locationIdentifier=OUTCODE%5E292&insId=1&radius=0.0&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=&displayPropertyType=&maxDaysSinceAdded=&_includeSSTC=on&sortByPriceDescending=&primaryDisplayPropertyType=&secondaryDisplayPropertyType=&oldDisplayPropertyType=&oldPrimaryDisplayPropertyType=&newHome=&auction=false"
# url_bs8 = "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=OUTCODE%5E295&insId=1&numberOfPropertiesPerPage=24&areaSizeUnit=sqft&googleAnalyticsChannel=buying"
# url_bs3 = "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=OUTCODE%5E277&insId=1&numberOfPropertiesPerPage=24&areaSizeUnit=sqft&googleAnalyticsChannel=buying"
# url_bs2 = "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=OUTCODE%5E266&insId=1&numberOfPropertiesPerPage=24&areaSizeUnit=sqft&googleAnalyticsChannel=buying"
# url_bs1 = "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=OUTCODE%5E259&insId=1&numberOfPropertiesPerPage=24&areaSizeUnit=sqft&googleAnalyticsChannel=buying"
# url_bs7 = "https://www.rightmove.co.uk/property-for-sale/find.html?locationIdentifier=OUTCODE%5E293&insId=1&numberOfPropertiesPerPage=24&areaSizeUnit=sqft&googleAnalyticsChannel=buying"


searches_post_codes = ["BS6", "BS8", "BS3", "BS2", "BS1", "BS7"]
searches_areas = ["St Andrews", "Redland", "Cotham+Montpelier", "Clifton", "Southville", "Bishopston"]
all_searches = [searches_post_codes, searches_areas]


def get_right_move_results(post_code):
    log.info(f"Getting {post_code} :::: {get_today()}")
    url = get_url(post_code)
    rm = RightmoveData(url)
    df = rm.get_results
    df = df.assign(Location=post_code)
    df = df.assign(date=get_today())
    return df

def combine_with_historic(df, post_code):
    if path.isfile(f'data/df_{post_code}.h5'):
        historic_df = pd.read_hdf(f'data/df_{post_code}.h5')
        log.info("Loaded previous, and combining")
        historic_df = historic_df.query("date > 20210502")
        df = pd.concat([historic_df, df])
        df = df.drop_duplicates(subset=['price', 'type', 'address', 'url', 'agent_url', 'postcode','number_bedrooms', 'Location', 'date'])
    else:
        log.info("File does not exist, returning original DataFrame")
    return df

def save_dataframe(df, post_code):
    df.to_hdf(f'data/df_{post_code}.h5', key=f'df_{post_code}', mode='w')

def apply_selection(df, selection):
    df = df.query(selection)
    return df

def get_percentage_change(medians):
    """Get the percentage change in median price."""
    change = medians[-1]/medians[0] * 100. - 100.
    return change

def convert_date_string(date):
    date = str(date)
    str_date = date[:4] + '-' + date[4:6] + '-' +date[6:]
    return str_date

def plot_trend(all_dfs, searches, number_bedrooms):
    """
    Plot the trend of average house price over time in each region.
    """
    regions_of_interest = ["Cotham+Montpelier", "Clifton", "Redland", "Southville"]
    fig, axs = plt.subplots(len(regions_of_interest), 1, sharex=True, sharey=True, figsize=(8, 10), dpi=120)
    fig.suptitle(f'Price Trends: {number_bedrooms} bedrooms')

    i = 0
    # import ipdb; ipdb.set_trace()
    for df, name in zip(all_dfs, searches):
        # Get the quantiles to plot
        if name in regions_of_interest:
            dates = df['date'].unique()
            means = []
            upper = []
            lower = []
            changes = []
            for date in dates:
                A = df.query(f'date == {date} & number_bedrooms == {number_bedrooms}')['price'].quantile([0.0, 0.25, 0.75, 1.0])
                lower.append(A[0.25])
                upper.append(A[0.75])
                A = df.query(f'date == {date} & number_bedrooms == {number_bedrooms}')['price'].median()
                number = df.query(f'date == {date} & number_bedrooms == {number_bedrooms}').shape[0]
                means.append(A)
            change = get_percentage_change(means)
            axs[i].plot(dates, means, label=f"£{means[-1]:.0f}k (+£{upper[-1] - means[-1]:.0f}k / -£{abs(means[-1] - lower[-1]):.0f}k) : N={number} : change: {change:.2f}%")
            axs[i].fill_between(dates, lower, upper, alpha=0.2)
            axs[i].legend(loc='upper left')
            axs[i].set_title(name)
            axs[i].set_ylabel("£k")
            i += 1
        else:
            continue
    labels = [convert_date_string(date) for date in dates]
    plt.xticks(dates, labels, rotation='vertical')
    fig.tight_layout()
    fig.subplots_adjust(top=0.88)
    plt.savefig(f'figs/trend_plot_{number_bedrooms}_beds.png', bbox_inches="tight")
    plt.close()

def plot_number(all_dfs, searches, number_bedrooms):
    """
    Plot the trend of average house price over time in each region.
    """
    regions_of_interest = ["Cotham+Montpelier", "Clifton", "Redland", "Southville"]
    # fig, axs = plt.subplots(len(regions_of_interest), 1, sharex=True, sharey=True, figsize=(8, 10), dpi=120)
    # fig.suptitle(f'Price Trends: {number_bedrooms} bedrooms')

    i = 0
    # import ipdb; ipdb.set_trace()
    for df, name in zip(all_dfs, searches):
        # Get the quantiles to plot
        if name in regions_of_interest:
            dates = df['date'].unique()
            means = []
            upper = []
            lower = []
            changes = []
            number = []
            for date in dates:
                number.append(df.query(f'date == {date} & number_bedrooms == {number_bedrooms}').shape[0])
                # lower.append(A[0.25])
                # upper.append(A[0.75])
                # A = df.query(f'date == {date} & number_bedrooms == {number_bedrooms}')['price'].median()
                # number = df.query(f'date == {date} & number_bedrooms == {number_bedrooms}').shape[0]
                # means.append(A)
            # change = get_percentage_change(means)
            # axs[i].plot(dates, means, label=f"£{means[-1]:.0f}k (+£{upper[-1] - means[-1]:.0f}k / -£{abs(means[-1] - lower[-1]):.0f}k) : N={number} : change: {change:.2f}%")
            # axs[i].fill_between(dates, lower, upper, alpha=0.2)
            # axs[i].set_title(name)
            # axs[i].set_ylabel("£k")
            plt.plot(dates, number, label=name)
            i += 1
        else:
            continue
    labels = [convert_date_string(date) for date in dates]
    plt.xticks(dates, labels, rotation='vertical')
    plt.tight_layout()
    # fig.subplots_adjust(top=0.88)
    plt.legend(loc='upper left')
    plt.savefig(f'figs/quantity_plot_{number_bedrooms}_beds.png', bbox_inches="tight")
    plt.close()


def main():
    log.info("Updating...")
    for search_type, name in zip(all_searches, ["searches_post_codes", "searches_areas"]):
        all_post_codes = []
        hisstoric_all_post_codes = []
        for search in search_type:
            df = get_right_move_results(search)
            all_post_codes.append(df)
            df_hist = combine_with_historic(df, search)
            hisstoric_all_post_codes.append(df_hist)
            save_dataframe(df_hist, search)
        if name == "searches_areas":
            for df in hisstoric_all_post_codes:
                df["price"] = df["price"]/1000.
            for beds in [1, 2, 3, 4]:
                plot_trend(hisstoric_all_post_codes, search_type, beds)
                plot_number(hisstoric_all_post_codes, search_type, beds)
        # import ipdb; ipdb.set_trace()
        log.info("Plotting...")
        selection = "number_bedrooms > 0 & number_bedrooms < 6 & price < 2E6"
        dfs = [apply_selection(df, selection) for df in all_post_codes]

        cdf = pd.concat(dfs)
        sns.boxplot(x="number_bedrooms", y="price", data=cdf, hue="Location", width=0.6, saturation=0.6)

        plt.legend()
        plt.savefig(f"figs/all_areas_all_bedrooms_{name}.png")
        plt.close()


        selection = "number_bedrooms > 0 & number_bedrooms <= 3 & price < 6.5E6"
        dfs = [apply_selection(df, selection) for df in all_post_codes]

        cdf = pd.concat(dfs)
        sns.boxplot(x="number_bedrooms", y="price", data=cdf, hue="Location", width=0.6, saturation=0.6)

        plt.legend()
        plt.ylim(0, 1.E6)
        plt.savefig(f"figs/all_areas_3_bedrooms_{name}.png")
        plt.close()




#
#
#
# # selection = "number_bedrooms == 2 & price < 0.5E6"
# # price_bins = np.linspace(0.22E6, 0.6E6, 21)
# #
# # log.info("Making selctions")
# # df_bs6 = df_bs6.query(selection)
# # df_bs8 = df_bs8.query(selection)
# #
# # plt.hist(df_bs6["price"], bins=price_bins, label="BS6", alpha=0.6)
# # plt.hist(df_bs8["price"], bins=price_bins, label="BS8", alpha=0.6)
# # plt.legend()
# # plt.show()
# #
# #
# # # plt.hist(df["number_bedrooms"])
# # # plt.show()
# #
# #
# #
# #
# #
# #
# # # locator = Nominatim(user_agent="myGeocoder")
# # # location = locator.geocode(“Champ de Mars, Paris, France”)
# # # print(“Latitude = {}, Longitude = {}”.format(location.latitude, location.longitude))
# # #
# # #
# # #
# # # coords_1 = (52.2296756, 21.0122287)
# # # coords_2 = (52.406374, 16.9251681)
# # #
# # # print(geopy.distance.vincenty(coords_1, coords_2).km)
# # #
# # #





# Options Parsing
parser = argparse.ArgumentParser(description='Functionality.')
parser.add_argument('--stand-alone', dest='stand_alone', action='store_true',
                    help='Run the script once now.')
parser.add_argument('--start-schedule', dest='start_schedule', action='store_true',
                    help='Run the script once now.')

args = parser.parse_args()
# # Want an option to select which postcodes
if args.start_schedule:
    log.info("In scheduler")
    schedule.every().day.at("11:00").do(main)
    # schedule.every(10).minutes.do(main)

    while True:
        log.info("Pending...")
        schedule.run_pending()
        time.sleep(60) # wait one minute
if args.stand_alone:
    main()
