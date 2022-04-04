"""
Creator: Naga Sneha Suresh
Created: 2022-03
Description: This script reads the csv source files and gives the hottest day details.
"""

import sys
import os
import pandas as pd
from datetime import date,datetime
from datetime import timedelta
import logging
from configparser import ConfigParser
import re
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
from pandera import Check, Column, DataFrameSchema



if __name__=="__main__":
    
    filepath=os.getcwd()
    ## Python variable should have the file location path. If not we can set it using os.chdir("Path")
    logging.basicConfig(filename=f'{filepath}/weather_{date.today()}.log',level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    logger = logging.getLogger(__name__)    

    try:
        logging.info("Try block")
        #parse config file  
        config=ConfigParser()
        config.read(f'{filepath}/options.cfg')
        src_file_path=config.get('General','src_file_path')
        src_file_path = f"{src_file_path}/"
        
        #Running the job for the availbale csv files at the given location
        output='Hi,'
        for f in os.listdir(src_file_path):
            month=f.split('.')[1]
            parquet_path=f'output_{month}.parquet'
            if re.match(r'^weather.*csv$',f): 
                #Reads csv file
                logging.info(f'Reading {f}')
                df_csv=pd.read_csv(f,index_col = None)
                
                #writes partitioned parquet files
                if os.path.exists(parquet_path):
                    logging.info("Removing existing parquet files")
                    shutil.rmtree(parquet_path)
                logging.info('Creating parquet files with partitions')
                df_par = pa.Table.from_pandas(df_csv)
                pq.write_to_dataset(df_par,root_path=parquet_path,partition_cols=['Country'],)
                logging.info('Created parquet files')
                
                #Reads from partitioned parquet files
                schema = DataFrameSchema(
                                {
                                    "ForecastSiteCode": Column(int),
                                    "ObservationTime": Column(int, nullable=True),
                                    "ObservationDate": Column(str, nullable=True),
                                    "WindDirection": Column(int, nullable=True),
                                    "WindSpeed": Column(int, nullable=True),
                                    "WindGust": Column(float, nullable=True),
                                    "Visibility": Column(float, nullable=True),
                                    "ScreenTemperature": Column(float, nullable=True),
                                    "Pressure": Column(float, nullable=True),
                                    "SignificantWeatherCode": Column(int, nullable=True),
                                    "SiteName": Column(str, nullable=True),
                                    "Latitude": Column(float, nullable=True),
                                    "Longitude": Column(float, nullable=True),
                                    "Region": Column(str, nullable=True)      
                                },
                            )
                
                logging.info('Reading parquet files')
                df=pd.read_parquet(parquet_path, engine='pyarrow')
                #validates schema of the files
                schema.validate(df)
                
                #Transformations
                max_day=df.loc[df['ScreenTemperature'] == df['ScreenTemperature'].max()]
                Hottest_date=max_day['ObservationDate'].to_string(index=False)
                Hottest_temperature=max_day['ScreenTemperature'].to_string(index=False)
                Hottest_region=max_day['Region'].to_string(index=False)
                 
                #Final Result
                output = output+f''' \n Please find below details of the hottest day of {month}:
                           Date of the hottest day: {Hottest_date}
                           Temperature of the hottest day: {Hottest_temperature}
                           Region of the hottest day: {Hottest_region}'''
        logging.info('Output printed on the console')       
        logging.info(output)
        
    except (Exception) as ex:
        logging.info(ex)
        
    finally:
        logging.info('Job ended.')
        logging.shutdown()

