# DataHealthCheck
Pyspark solution to check the health of data

DataHealthCheck:

Solution is developed using Pyspark and python, leveraged python libraries for Data Visualization. 

1)  Config file has following sections input_file, output_directory, log_directory
			a) input_file section needs below details
				i)   source type - currently following file types only supported csv, json, ORC, parquet and database. Enter values as given
				ii)  source directory - Absolute path of source file should be given. For database this is not needed
				iii) primary key - if no primary key leave it blank. if multiple values enter comma seperated values.

				[input_file]
                                src_type = csv
                                src_file_path = E:\\Capgemini Tech Challenge\\Inputfiles\\appl_stock-7.csv
                                primary_key = Date,Open

			b) output_directory needs the absolute path where results should be saved.
				[output_dir]
				output_path = E:\\Capgemini Tech Challenge\\Output

			c) log_directory needs absolute file path where logs should be maintained
				[log_dir]
				log_path = E:\\Capgemini Tech Challenge\logs\\logger.txt

2) data_pipeline.py has the main function which reads the configuration files and instantiates the Pipeline class.
   Pipeline class has run_pipeline method which interacts with Other classes for data extraction, performing health check and storing the results.

3) ingest.py - based on the source type and source path, it reads the input and sends the data as dataframe to driver program.
	     - if source type is database, it gets the required details from db.ini config file which has host, driver, table, username and password details

4) transform.py - it has methods to perform various health check at table level and column level and send the information to driver program.

5) persist.py - it creates output directory based on the information given in config file.
		- gets data from driver program and creates reports, plots, charts in given output directory.



Submitting the Job:

pip install pandas
pip install seaborn

spark-submit data_pipeline.py
(or)
spark-submit --master local[*] --deploy-mode client data_pipeline.py

--num-executor --executormemory --executor-cores --driver-memory can be given while deploying in cluster

spark-3.1.2


refer: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1689922132651787/3273963528173652/5432158817586684/latest.html

Above is the databricks solution which can be used, else data_health_check.py can be deployed in cluster and Scheduled.
