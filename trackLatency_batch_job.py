# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 15:19:43 2022

@author: nikhil
"""

import os
import time
import datetime

import pandas as pd

import helper



check_ever_n_seconds = 1


## path to dump the output
DOWNLOAD_DIR = "../DataDump/.tmp//"

## path to write the csv
WRITE_CSV_DIR = "../DataDump/.tmp//"



####################################################################
####################################################################

radProduct = 'ABI-L1b-RadC' ## L1 products CONUS

## Level 2 data product to check
dataProductCode = [ 'ABI-L2-MCMIPC', ## Level 2 Cloud and Moisture Imagery CONUS
                    'ABI-L2-ACMC',   ## Level 2 Clear Sky Mask CONUS
                    'ABI-L2-FDCC'    ## Level 2 Fire (Hot Spot Characterization) CONUS
                ]




####################################################################
####################################################################

def trackLatency(dataProductCode, check_ever_n_seconds = 1, download = True, verbose = False):

    ## var init
    counter = 0
    dict_product_avaliable_time = {}
    returnDict = {}

    queryTime = datetime.datetime.now(datetime.timezone.utc)
    start_time, end_time = helper.getTimeRange(queryTime, bufferMinutes = [0, 8])

    returnDict["Query Time"] = queryTime



    ## Wait till all 16 bands of Level 1 Rad product is available
    while True:
        df = helper.getProductList(start_time, end_time, product = radProduct, satellite = 16)

        # Once all 16 bands are available
        if len(df) >= 16:
            # Note the time 
            dict_product_avaliable_time[radProduct] = datetime.datetime.now(datetime.timezone.utc)

            returnDict["Frame Start Time"] = df['start'].mean()
            returnDict["Frame End Time"]   = df['end'].mean()
            returnDict["L1 Rad Created"] = df.iloc[-1]['creation']
            returnDict["L1 Rad Available"] = dict_product_avaliable_time[radProduct]


            if verbose:
                print('')
                print("Frame Start: ", df['start'].mean().strftime('%H:%M:%S.%f'), '+/-', df['start'].std().seconds + df['start'].std().microseconds/1E6)
                print("Frame End  : ", df['end'].mean().strftime('%H:%M:%S.%f'),   '+/-', df['end'].std().seconds + df['end'].std().microseconds/1E6  )
                print('')
                
                frameEndTime = df['end'].mean()
                print("Level 1 Created           --> {} --> {} seconds.".format(df.iloc[-1]['creation'], (df.iloc[-1]['creation'] - frameEndTime).seconds))
                print("Level 1 Available for us  --> {} --> {} seconds.".format(dict_product_avaliable_time[radProduct], (dict_product_avaliable_time[radProduct] - frameEndTime).seconds))
                counter += 1

            counter += 1
            break

        if verbose:
            print ("Waiting for product....  --> {}".format(datetime.datetime.now(datetime.timezone.utc)), end="\r")

        time.sleep(check_ever_n_seconds)




    ## Wait till all Level 2 product in dataProductCode gets available
    while counter < len(dataProductCode):

        ## Reset counter
        counter = 0

        for p in dataProductCode:
            df = helper.getProductList(start_time, end_time, product = p, satellite = 16)

            if len(df) > 0:
                if p not in dict_product_avaliable_time.keys():
                    dict_product_avaliable_time[p] = datetime.datetime.now(datetime.timezone.utc)

                    returnDict["L2 " + p + " Created"] = df.iloc[-1]['creation']
                    returnDict["L2 " + p + " Available"] = dict_product_avaliable_time[p]

                    if verbose:
                        print(df.iloc[-1]['file'])
                        print("Level 2 {} Created          --> {} --> {} seconds.".format(p, df.iloc[-1]['creation'], (df.iloc[-1]['creation'] - frameEndTime).seconds))
                        print("Level 2 {} Available for us --> {} --> {} seconds.".format(p, dict_product_avaliable_time[p], (dict_product_avaliable_time[p] - frameEndTime).seconds))
                        print("-------------------------------\n")
                    
                counter += 1  
        time.sleep(check_ever_n_seconds)



    ## Download L1 Rad product and L2 'ABI-L2-MCMIPC' only
    if download:
        
        ## Download RAD 
        df = helper.getProductList(start_time, end_time, product = 'ABI-L1b-RadC', satellite = 16)
        df = df[df["band"].isin([2, 7, 13, 14, 15])]
        uniqueStartTimes = df['start'].unique()

        helper.download(df = df, BASEDIR = DOWNLOAD_DIR, startTime = uniqueStartTimes[-1], makeGeoTiff = False)
        returnDict["L1 Rad Downloaded"] = datetime.datetime.now(datetime.timezone.utc)
        

        ## Download RAD L2 'ABI-L2-MCMIPC' only
        if 'ABI-L2-MCMIPC' in dataProductCode:
            df = helper.getProductList(start_time, end_time, product = 'ABI-L2-MCMIPC', satellite = 16)
            uniqueStartTimes = df['start'].unique()

            helper.download(df = df, BASEDIR = DOWNLOAD_DIR, startTime = uniqueStartTimes[-1], makeGeoTiff = False)
            returnDict["L2 " + p + " Downloaded"] = datetime.datetime.now(datetime.timezone.utc)


    # Track time 
    returnDict["Total Time"] = (datetime.datetime.now(datetime.timezone.utc) - returnDict["Frame End Time"]).seconds

    return returnDict



####################################################################
####################################################################



latencyDict = trackLatency(dataProductCode, check_ever_n_seconds = check_ever_n_seconds, verbose = True)

## Create a CSV
df = pd.DataFrame(latencyDict, index=[0])


for i in range(500):
    print("---------------------------------------")
    # latencyDict = trackLatency(dataProductCode, check_ever_n_seconds = check_ever_n_seconds, verbose = True)

    for j in range(500):  
        extraVerbose = False
        if j > 0:
            extraVerbose = True

        try:
            latencyDict = trackLatency(dataProductCode, check_ever_n_seconds = check_ever_n_seconds, verbose = True, extraVerbose = extraVerbose)
            break
        except:
            ## wait till next product gets available
            print(datetime.datetime.now(datetime.timezone.utc), "Exception Occured. Trying Again.. " + str(j))
            time.sleep(5)


    df =  pd.concat([df, pd.DataFrame(latencyDict, index=[0])])
    df.to_csv(WRITE_CSV_DIR + "/latency" + str(i) + ".csv")



            



