import pyspark
import pandas as pd
import json
from pyspark.sql.functions import count, when, isnan, col, stddev, sum, countDistinct, min, max, length, variance, mean
import logging

class Transform:

    def __init__(self, spark, col_null, row_count, df):
        self.spark = spark
        self.col_null = col_null
        self.row_count = row_count
        self.df = df

    def set_primary_key(self, prim_key):
        try:
            if prim_key == '':
                logging.info("The given value for primary key is empty")
                return None
            else:
                logging.info("Setting primary key")
                return prim_key.split(',')
        except Exception as ex:
            logging.error("setting primary key failed due to "+ex)

    def no_of_empty_columns(self, col_null, row_count):
        """This function requires table as input, identifies number of empty columns in the table
           Returns the number of empty columns"""
        logging.info("Identifying number of empty columns in a table has been started")
        try:
            null_cols = 0
            for i in col_null[0]:
                if i == row_count:
                    null_cols += 1
            logging.info("Identifying number of empty columns in a table is completed successfully")
            return null_cols
        except Exception as ex:
            logging.error("Identifying number of empty columns in a table has an Exception " + ex)
            return 0

    def calculate_completeness(self, col_null, row_count, total_records):
        """This function requires table as input, calculates the completeness in the table
        Returns the completeness of the table"""
        logging.info("Calculate the completeness of the table has been started")
        try:
            not_null_count = 0
            for i in col_null[0]:
                not_null_count = row_count + not_null_count - i
            completeness = (not_null_count / total_records) * 100
            logging.info("Calculating completeness of the table is completed successfully")
            return completeness
        except Exception as ex:
            logging.error("Calculating completeness of the table has an Exception " + ex)
            return 0

    def calculate_completeness_empt_col_exclude(self, col_null, row_count, null_cols, df):
        """This function requires table as input, calculates the completeness by
        excluding empty columns from the table
        Returns the completeness by excluding empty columns from the table"""
        logging.info("Calculating completeness by excluding empty columns from the table has been started")
        try:
            not_null_count_ex = 0
            for i in col_null[0]:
                if i != row_count:
                    not_null_count_ex = row_count + not_null_count_ex - i
            total_records = row_count * (len(df.columns) - null_cols)
            completeness_excl_null = (not_null_count_ex / total_records) * 100
            logging.info("Calculating completeness by excluding empty columns from the table is completed successfully")
            return completeness_excl_null
        except Exception as ex:
            logging.error("Calculating completeness by excluding empty columns from the table has an Exception " + ex)
            return 0

    def calculate_uniqueness(self, df, primary_key, row_count):
        """This function requires table as input, calculates the uniqueness of the table
        Returns the uniqueness percentage of the table"""
        logging.info("Calculating uniqueness of the table has been started")
        try:
            all_columns = set(df.columns)
            if primary_key is None:
                non_primary_columns = list(all_columns)
            else:
                non_primary_columns = list(all_columns.difference(set(primary_key)))
            uniqueness = df.select(non_primary_columns).distinct().count()
            unique_percent = (uniqueness / row_count) * 100
            logging.info("Calculating uniqueness of the table is completed successfully")
            return unique_percent
        except Exception as ex:
            logging.error("Calculating uniqueness of the table has an Exception " + ex )
            return 0

    def find_dtype(self, col_name):
        """This function requires column name as input, finds the datatype of the column
        Returns the datatype of the column"""
        logging.info("Finding the datatype of the given column - " +col_name)
        try:
            dtype = dict(self.df.dtypes)[col_name]
            logging.info("Finding the data fill ratio of the given column is Completed successfully for the column -" +col_name)
            return dtype
        except Exception as ex:
            logging.error("Finding the datatype of the given column has an Exception " + ex)
            return 0

    def find_data_fill(self, col_name):
        """This function requires column name as input, finds the data fill ratio of the column
        Returns the data fill ratio of the column"""
        logging.info("Finding the data fill ratio of the given column has been started  for the column -" +col_name)
        try:
            number_of_null = self.col_null[0][col_name]
            data_fill_ratio = ((self.row_count - number_of_null) / self.row_count) * 100
            logging.info("Finding the data fill ratio of the given column is Completed successfully  for the column -" +col_name)
            return data_fill_ratio
        except Exception as ex:
            logging.error("Finding the data fill ratio of the given column has an Exception " + ex)
            return 0

    def find_nonempty_count(self, col_name):
        """This function requires column name as input, finds the non empty count of the column
        Returns the non empty count of the column"""
        logging.info("Finding non empty count of the given column has been started  for the column -" +col_name)
        try:
            number_of_null = self.col_null[0][col_name]
            non_empty_count = self.df.count() - number_of_null
            logging.info("Finding non empty count of the given column is completed successfully  for the column -" +col_name)
            return non_empty_count
        except Exception as ex:
            logging.error("Finding non empty count of the given column has an Exception " + ex)
            return 0

    def find_duplicate_count(self, col_name):
        """This function requires column name as input, finds duplicate count of the column
        Returns the duplicate count of the column"""
        logging.info("Finding the duplicate count of the given column has been started  for the column -" +col_name)
        try:
            val = self.df.groupBy(col_name).count().where(col('count') > 1).select(sum('count')).collect()[0][0]
            if val is None:
                logging.info("Finding the duplicate count of the given column is completed successfully  for the column -" + col_name)
                return 0
            else:
                logging.info("Finding the duplicate count of the given column is completed successfully  for the column -" + col_name)
                return val
        except Exception as ex:
            logging.error("Finding the duplicate count of the given column has an Exception " + ex)
            return 0

    def find_duplicate_count(self, col_name):
        """This function requires column name as input, finds duplicate count of the column
        Returns the duplicate count of the column"""
        logging.info("Finding the duplicate count of the given column has been started for the column -" +col_name)
        try:
            val = self.df.groupBy(col_name).count().where(col('count') > 1).select(sum('count')).collect()[0][0]
            if val is None:
                logging.info("Finding the duplicate count of the given column is completed successfully for the column -" +col_name)
                return 0
            else:
                logging.info("Finding the duplicate count of the given column is completed successfully for the column -" +col_name)
                return val
        except Exception as ex:
            logging.error("Finding the duplicate count of the given column has an Exception " + ex)
            return 0

    def find_unique_count(self, col_name):
        """This function requires column name as input, finds unique count of the column
        Returns the unique count of the column"""
        logging.info("Finding unique count of the given column has been started for the column -" +col_name)
        try:
            val = self.df.select(countDistinct(col_name)).collect()[0][0]
            if val is None:
                logging.info("Finding unique count of the given column is completed successfully for the column -" + col_name)
                return 0
            else:
                logging.info("Finding unique count of the given column is completed successfully for the column -" + col_name)
                return val
        except Exception as ex:
            logging.error("Finding unique count of the given column has an Exception " + ex)
            return 0

    def find_min_value(self, col_name):
        """This function requires column name as input, finds minimum value of the column
        Returns the minimum value of the column"""
        logging.info("Finding minimum value of the given column has been started for the column - " +col_name)
        try:
            val = self.df.select(min(col_name)).collect()[0][0]
            if val is None:
                logging.info("Finding minimum value of the given column is completed successfully for the column - " +col_name)
                return 0
            else:
                logging.info("Finding minimum value of the given column is completed successfully for the column - " +col_name)
                return val
        except Exception as ex:
            logging.error("Finding minimum value of the given column has an Exception " + ex)
            return 0

    def find_max_value(self, col_name):
        """This function requires column name as input, finds maximum value of the column
        Returns the maximum value of the column"""
        logging.info("Finding maximum value of the given column has been started for the column - " +col_name)
        try:
            val = self.df.select(max(col_name)).collect()[0][0]
            if val is None:
                logging.info("Finding maximum value of the given column is completed successfully for the column - " +col_name)
                return 0
            else:
                logging.info("Finding maximum value of the given column is completed successfully for the column - " +col_name)
                return val
        except Exception as ex:
            logging.error("Finding maximum value of the given column has an Exception " + ex)
            return 0

    def find_min_len(self, col_name):
        """This function requires column name as input, finds minimum length of the column
        Returns the minimum length of the column"""
        logging.info("Finding minimum length of the given column has been started  for the column -" +col_name)
        try:
            val = self.df.withColumn('newcolumn', length(self.df[col_name])).select(min('newcolumn')).collect()[0][0]
            if val is None:
                logging.info("Finding minimum length of the given column is completed successfully  for the column -" +col_name)
                return 0
            else:
                logging.info("Finding minimum length of the given column is completed successfully  for the column -" +col_name)
                return val
        except Exception as ex:
            logging.error("Finding minimum length of the given column has an Exception " + ex)
            return 0

    def find_max_len(self, col_name):
        """This function requires column name as input, finds maximum length of the column
        Returns the maximum length of the column"""
        logging.info("Finding maximum length of the given column has been started  for the column -" +col_name)
        try:
            val = self.df.withColumn('newcolumn', length(self.df[col_name])).select(max('newcolumn')).collect()[0][0]
            if val is None:
                logging.info("Finding maximum length of the given column is completed successfully  for the column -" +col_name)
                return 0
            else:
                logging.info("Finding maximum length of the given column is completed successfully  for the column -" +col_name)
                return val
        except Exception as ex:
            logging.error("Finding maximum length of the given column has an Exception " + ex)
            return 0

    def find_average(self, col_name):
        """This function requires column name as input, finds average value of the column
        Returns the average value of the column"""
        logging.info("Finding average value of the given column has been started  for the column -" +col_name)
        try:
            if dict(self.df.dtypes)[col_name] in ['int', 'double', 'bigint', 'float', 'long', 'decimal.Decimal']:
                avg = self.df.select(mean(col_name)).collect()[0][0]
                logging.info("Finding average value of the given column is completed successfully  for the column -" +col_name)
                return avg
            else:
                logging.info("Finding average value of the given column is completed successfully  for the column -" +col_name)
                return 0
        except Exception as ex:
            logging.error("Finding average value of the given column has an Exception " + ex)
            return 0

    def find_stddev(self, col_name):
        """This function requires column name as input, finds standard deviation of the column
        Returns the standard deviation of the column"""
        logging.info("Finding standard deviation of the given column has been started  for the column -" +col_name)
        try:
            if dict(self.df.dtypes)[col_name] in ['int', 'double', 'bigint', 'float', 'long', 'decimal.Decimal']:
                stdev = self.df.select(stddev(col_name)).collect()[0][0]
                logging.info("Finding standard deviation of the given column is completed successfully  for the column -" +col_name)
                return stdev
            else:
                logging.info("Finding standard deviation of the given column is completed successfully  for the column -" +col_name)
                return 0
        except Exception as ex:
            logging.error("Finding standard deviation of the given column has an Exception " + ex)
            return 0

    def find_median(self, col_name):
        """This function requires column name as input, finds median of the column
        Returns the median of the column"""
        logging.info("Finding median value of the given column has been started  for the column -" +col_name)
        try:
            mid_value = (self.df.count() // 2)
            self.df.createOrReplaceTempView('temptable')
            if dict(self.df.dtypes)[col_name] in ['int', 'double', 'bigint', 'float', 'long', 'decimal.Decimal']:
                if (self.row_count % 2) == 1:
                    val = self.spark.sql(
                        "Select {}, row_number() over(Order by {} desc) as row from temptable".format(col_name,
                                                                                                      col_name)).filter(
                        col('row') == mid_value).collect()[0][0]
                    logging.info("Finding median value of the given column is completed successfully  for the column -" +col_name)
                    return val
                else:
                    val1 = self.spark.sql(
                        "Select {}, row_number() over(Order by {} desc) as row from temptable".format(col_name,
                                                                                                      col_name)).filter(
                        col('row') == mid_value).collect()[0][0]
                    val2 = self.spark.sql(
                        "Select {}, row_number() over(Order by {} desc) as row from temptable".format(col_name,
                                                                                                      col_name)).filter(
                        col('row') == mid_value + 1).collect()[0][0]
                    val = (val1 + val2) / 2
                    logging.info("Finding median value of the given column is completed successfully  for the column -" +col_name)
                    return val
            else:
                logging.info("Finding median value of the given column is completed successfully  for the column -" +col_name)
                return 0
        except Exception as ex:
            logging.info("Finding median value of the given column has an Exception " + ex)
            return 0

    def find_variance(self, col_name):
        """This function requires column name as input, finds variance of the column
        Returns the variance of the column"""
        logging.info("Finding variance of the given column has been started  for the column -" +col_name)
        try:
            if dict(self.df.dtypes)[col_name] in ['int', 'double', 'bigint', 'float', 'long', 'decimal.Decimal']:
                var =self.df.select(variance(col_name)).collect()[0][0]
                logging.info("Finding variance of the given column is completed successfully  for the column -" +col_name)
                return var
            else:
                logging.info("Finding variance of the given column is completed successfully  for the column -" +col_name)
                return 0
        except Exception as ex:
            logging.error("Finding variance of the given column has an Exception " + ex)
            return 0

    def generate_columnwise_report(self):
        logging.info("Generating column wise summary report")
        try:
            content = []
            for col_name in self.df.columns:
                info = dict()
                info['dtype'] = Transform.find_dtype(self, col_name)
                info['dfill'] = Transform.find_data_fill(self, col_name)
                info['NonEmt'] = Transform.find_nonempty_count(self, col_name)
                info['emtCnt'] = self.col_null[0][col_name]
                info['dupCnt'] = Transform.find_duplicate_count(self, col_name)
                info['uniqCnt'] = Transform.find_unique_count(self, col_name)
                info['minVal'] = Transform.find_min_value(self, col_name)
                info['maxVal'] = Transform.find_max_value(self, col_name)
                info['minLen'] = Transform.find_min_len(self, col_name)
                info['maxLen'] = Transform.find_max_len(self, col_name)
                info['stdDev'] = Transform.find_stddev(self, col_name)
                info['average'] = Transform.find_average(self, col_name)
                info['median'] = Transform.find_median(self, col_name)
                info['variance'] = Transform.find_variance(self, col_name)
                content.append(info)
            column_summary = pd.DataFrame(content, index=self.df.columns)
            logging.info("Column wise summary report has been generated successfully")
            return column_summary
        except Exception as ex:
            logging.error("Columnwise summary creation has an Exception " + ex)




