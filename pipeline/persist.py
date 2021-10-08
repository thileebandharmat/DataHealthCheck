import pyspark
from datetime import datetime
import pandas as pd
import time
import os
import seaborn as sns
import matplotlib.pyplot as plt
import logging

class Persist:
    def __init__(self, spark, df2):
        self.spark = spark
        self.df2 = df2

    def create_output_directory(self, output_path):
        """This function creates directory to save output files, directory name and path are hardcoded
        and returns the same"""
        try:
            logging.info("Creating output directory to save output files")
            now = datetime.now()
            dt_string = now.strftime("%Y%m%d%H%M%S")
            directory = "DataHealthReport_" + dt_string
            parent_dir = output_path
            self.report_directory = os.path.join(parent_dir, directory)
            os.mkdir(os.path.join(self.report_directory))
            logging.info("Output path has been created successfully to store output files")
        except Exception as ex:
            logging.error("Output directory creation failed due to " + ex)

    def generate_table_health_report(self, col_count, row_count, empty_columns_count, completeness,
                                     completeness_excl_empty, uniqueness_percent, health_score):
        """This function uses the global variables which has health summary of the table
        and creates a text file with those information"""
        try:
            logging.info("Generating health summary of the table")
            line_1 = "Number of columns in the data source is: " + str(col_count)
            line_2 = "Total number of records in the table is: " + str(row_count)
            line_3 = "No. of Empty columns: " + str(empty_columns_count)
            line_4 = "Completeness percent is: {0:.2f}%".format(completeness)
            line_5 = "Completeness excluding Empty columns percent is: {0:.2f}%".format(completeness_excl_empty)
            line_6 = "Uniqueness percent is: {0:.2f}%".format(uniqueness_percent)
            line_7 = "Health Score is: {0:.2f}%".format(health_score)
            with open(self.report_directory + '/Table_health_report.txt', 'w') as out:
                out.write('{}\n{}\n{}\n{}\n{}\n{}\n{}\n'.format(line_1, line_2, line_3, line_4, line_5, line_6, line_7))
            logging.info("Health summary of the table has been generated successfully")
        except Exception as ex:
            logging.error("Table health report creation failed due to " + ex)

    def create_table_health_plot(self, completeness, completeness_excl_empty, uniqueness_percent, health_score):
        """This function uses the global variables which has health summary of the table
        and creates a plot and saves it in a specified path"""
        try:
            logging.info("Creating health plot of the table and saving it in a specified path")
            x = ['C_ness', 'CnesXemt', 'unique_%', 'H_score']
            y = [completeness, completeness_excl_empty, uniqueness_percent, health_score]
            plt.barh(x, y)
            for index, value in enumerate(y):
                plt.text(value, index, round(value, 2))
            plt.title("Health Index of table")
            plt.xlabel("Percentage")
            plt.savefig(self.report_directory + '/Health_index_of_table.png', dpi=200)
            plt.close()
            logging.info("Health plot of the table has been created and saved successfully")
        except Exception as ex:
            logging.error("Table health plot creation failed due to " + ex)

    def save_columnwise_report(self):
        """This function uses the global variables which has health summary t column level
                and creates a csv file and saves it in a specified path"""
        try:
            logging.info("Storing column wise summary report")
            self.df2.to_csv(self.report_directory + '/columnwise_summary.csv')
            logging.info("Column wise summary report has been stored successfully")
        except Exception as ex:
            logging.error("Columnwise attribute report creation failed due to " + ex)

    def create_attributes_plot_1(self):
        """This function uses the column wise health report and
                        and creates a chart for unique count and non-empty count and saves it in a specified path"""
        try:
            plt.plot(self.df2.index.values, self.df2['uniqCnt'], label='Count of Unique records', color='y')
            plt.plot(self.df2.index.values, self.df2['NonEmt'], label='Count of Non_empty', color='g')
            plt.xlabel("Attributes")
            plt.ylabel("count")
            plt.legend(["uniq_count", "non_empty"])
            plt.title("Unique and Non empty record count")
            plt.savefig(self.report_directory + '/Unique_and_non_empty_count.png', dpi=200)
            plt.close()
        except Exception as ex:
            logging.error("Unique and non-empty plot creation failed due to " + ex)

    def create_attributes_plot_2(self):
        """This function uses the column wise health report and
                                and creates a chart for duplicate count and empty count and saves it in a specified path"""
        try:
            plt.plot(self.df2.index.values, self.df2['emtCnt'], label='Count of empty records', color='r')
            plt.plot(self.df2.index.values, self.df2['dupCnt'], label='Count of Duplicates', color='b')
            plt.xlabel("Attributes")
            plt.ylabel("count")
            plt.legend(["empty_count", "duplicate_count"])
            plt.title("Empty and Duplicate records count")
            plt.savefig(self.report_directory + '/Empty_and_duplicate_records_count.png', dpi=200)
            plt.close()
        except Exception as ex:
            logging.error("Duplicate and empty plot creation failed due to " + ex)

    def create_attributes_heat_map(self):
        """This function uses the column wise health report, normalizes the data,
            creates a heat map and saves it in a specified path"""
        try:
            data = self.df2[['dfill', 'NonEmt', 'emtCnt', 'dupCnt', 'uniqCnt', 'minLen',
                        'maxLen', 'stdDev', 'average', 'median', 'variance']]
            df_min_max_scaled = data.copy()
            # apply normalization techniques
            for column in df_min_max_scaled.columns:
                df_min_max_scaled[column] = (df_min_max_scaled[column] - df_min_max_scaled[column].min()) / (
                        df_min_max_scaled[column].max() - df_min_max_scaled[column].min())
            # view normalized data
            cmap = "tab20"
            heatmap = sns.heatmap(data=df_min_max_scaled,
                                  cmap=cmap)
            heatmap.set_xticklabels(heatmap.get_xticklabels(), rotation=30)
            heatmap.set_yticklabels(heatmap.get_yticklabels(), rotation=30)
            plt.title("Heat map of Attributes Vs features")
            plt.savefig(self.report_directory + '/HeatMap_AttributesVsFeatures.png', dpi=200)
            plt.close()
        except Exception as ex:
            logging.error("Heat map creation failed due to " + ex)