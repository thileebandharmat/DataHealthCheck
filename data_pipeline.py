import sys
from pyspark.sql import SparkSession
from pipeline import transform, ingest, persist
from pyspark.sql.functions import count, when, isnan, col
import configparser
import logging
import logging.config

class Pipeline:


    def create_spark_session(self):
        self.spark = SparkSession.builder.appName("Data Health Check").getOrCreate()

    def run_pipeline(self, src_type, src_file_path, prim_key, output_path, log_path):
        """Data Ingestion - Reads config file and reads data"""
        #src_type, src_file_path, prim_key, output_path, log_path = Pipeline.read_config_files(self)
        logging.info("running pipeline 2")
        ingest_process = ingest.Ingestion(self.spark)
        df = ingest_process.ingest_hetrogenous_source(src_type, src_file_path)
        df = df.cache()
        row_count = df.count()
        col_count = len(df.columns)
        total_records = row_count * col_count
        col_null = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).collect()
        """Data Transformation - Contains functions to perform various calculation"""
        transform_process = transform.Transform(self.spark, col_null, row_count, df)
        primary_key = transform_process.set_primary_key(prim_key)
        empty_columns_count = transform_process.no_of_empty_columns(col_null, row_count)
        completeness = transform_process.calculate_completeness(col_null, row_count, total_records)
        completeness_excl_empty = transform_process.calculate_completeness_empt_col_exclude(col_null, row_count, empty_columns_count, df)
        uniqueness_percent = transform_process.calculate_uniqueness(df, primary_key, row_count)
        health_score = (uniqueness_percent + completeness) / 2
        df2 = transform_process.generate_columnwise_report()
        """Data Persists - Contains functions to save reports and charts"""
        persist_process = persist.Persist(self.spark, df2)
        persist_process.create_output_directory(output_path)
        persist_process.generate_table_health_report(col_count, row_count, empty_columns_count, completeness,
                                     completeness_excl_empty, uniqueness_percent, health_score)
        persist_process.create_table_health_plot(completeness, completeness_excl_empty, uniqueness_percent, health_score)
        persist_process.save_columnwise_report()
        persist_process.create_attributes_plot_1()
        persist_process.create_attributes_plot_2()
        persist_process.create_attributes_heat_map()


if __name__ == '__main__':
    print("Process started")
    config = configparser.ConfigParser()
    config.read('config\config.ini')
    log_path = config['log_dir']['log_path']
    logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s -  %(levelname)s -  %(message)s')
    src_type, src_file_path, prim_key, output_path = config['input_file']['src_type'], config['input_file']['src_file_path'], config['input_file']['primary_key'],config['output_dir']['output_path']
    logging.info("Instantiating Pipeline.,")
    pipeline = Pipeline()
    logging.info("Pipeline Instantiated.,")
    #src_type, src_file_path, prim_key, output_path, log_path = Pipeline.read_config_files()
    pipeline.create_spark_session()
    logging.info("running pipeline")
    pipeline.run_pipeline(src_type, src_file_path, prim_key, output_path, log_path)
    logging.info("Process ended successfully")
    print("Process ended")


