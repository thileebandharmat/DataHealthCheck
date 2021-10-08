import pyspark
import json
import sys
import logging
import configparser


class Ingestion:

    def __init__(self, spark):
        self.spark = spark

    def read_db_properties(self):
        """This function read database properties from the config file and
        returns the details"""
        try:
            logging.info("Reading db properties started")
            config = configparser.ConfigParser()
            config.read('config\db.ini')
            host = config['mysql']['host']
            driver = config['mysql']['driver']
            table = config['mysql']['table']
            user = config['mysql']['user']
            passwd = config['mysql']['passwd']
            logging.info("Reading db properties completed successfully")
            return host, driver, table, user, passwd
        except Exception as ex:
            logging.error("Reading dbproperties failed due to " + ex)

    def ingest_hetrogenous_source(self, src_type, src_file_path):
        """ This function uses the input type and input file path from the config file.
        reads the data from the source and returns the dataframe"""
        logging.info("Reading input file started")
        if src_type == 'csv':
            try:
                logging.info("The given input file is " + src_type + "format")
                sourcefile_path = src_file_path
                return self.spark.read.csv(sourcefile_path, inferSchema=True, header=True)
            except Exception as ex:
                logging.error("Input read failed due to " + ex)
                sys.exit()
        elif src_type == 'database':
            try:
                logging.info("The given input is table format")
                host, driver, table, user, passwd = Ingestion.read_db_properties(self)
                return self.spark.read.format("jdbc").option("url", host) \
                    .option("driver", driver).option("dbtable", table) \
                    .option("user", user).option("password", passwd).load()
            except Exception as ex:
                logging.error("Input read failed due to " + ex)
                sys.exit()
        elif src_type == 'json':
            try:
                logging.info("The given input file is " + src_type + "format")
                sourcefile_path = src_file_path
                return self.spark.read.json(sourcefile_path)
            except Exception as ex:
                logging.error("Input read failed due to " + ex)
                sys.exit()
        elif src_type == 'ORC':
            try:
                logging.info("The given input file is " + src_type + "format")
                sourcefile_path = src_file_path
                return self.spark.read.orc(sourcefile_path)
            except Exception as ex:
                logging.error("Input read failed due to " + ex)
                sys.exit()
        elif src_type == 'parquet':
            try:
                logging.info("The given input file is " + src_type + "format")
                sourcefile_path = src_file_path
                return self.spark.read.parquet(sourcefile_path)
            except Exception as ex:
                logging.error("Input read failed due to " + ex)
                sys.exit()
        else:
            logging.info("Selected file type not supported, please try with given formats alone")
            sys.exit()
