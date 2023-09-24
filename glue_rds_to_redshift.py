import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.context import SparkConf
from pyspark.sql.functions import *
from awsglue.transforms import Join, SelectFields



class JobBase(object):


    # Set fairscheduler.xml from S3 as config file
    fair_scheduler_config_file= "fairscheduler.xml"

    def execute(self):

        self.__start_spark_glue_context()
    
        args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])
        self.logger = self.glue_context.get_logger()
        self.logger.info("Starting Glue Threading job ")
    		
    		# Reading from Crawled catalog tables and create DataFrames
        transactions_df = self.glue_context.create_dynamic_frame.from_catalog(database="mysql_to_redshift", table_name="trial_register__transaction",
                                                                                    redshift_tmp_dir=args["TempDir"],
                                                                                    transformation_ctx="datasource0")
        users_df = self.glue_context.create_dynamic_frame.from_catalog(database="mysql_to_redshift", table_name="users__dimension",
                                                                                redshift_tmp_dir=args["TempDir"],
                                                                                transformation_ctx="datasource0")
        
        subscriptions_df = self.glue_context.create_dynamic_frame.from_catalog(database="mysql_to_redshift", table_name="subscriptions__dimension",
                                                                                    redshift_tmp_dir=args["TempDir"],
                                                                                    transformation_ctx="datasource0")

		# Joining created dataframes and save it as parameter

        transactions_df = transactions_df.rename_field('account_id', 'trn_account_id').rename_field('subscription_id', 'trn_subscription_id')
        #subscriptions_df = securities_df.rename_field('name', 'security_name')
        joined_df= Join.apply(Join.apply(transactions_df, users_df, 'acount_id', 'trn_account_id'), subscriptions_df, 'subscription_id', 'trn_security_id')
        selected_df = SelectFields.apply(frame = joined_df, paths = ['transaction_id', 'account_id', 'subscription_id', "paid_amount","register_date", "subscription_type","subscription_name","subscription_register_type","subscription_code","subscription_release","subscription_end"] ).toDF()
        selected_df.cache()
    		
        import concurrent.futures

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
            # calling method using Python thread
        executor.submit(self.__create_subscriptions_report,sc=selected_df)
        executor.submit(self.__create_accounts_report,sc=selected_df)
        self.logger.info("Completed Threading job")

    def __create_users_report(self, selected_df):
        self.logger.info("Starting account reports..")
		#  set pool local to 1
        self.sc.setLocalProperty("spark.scheduler.pool", str("1"))
        transactions_by_users_report_df = selected_df.groupBy('account_id', 'holder_name', "date").agg(sum("amount").alias("total")).repartition(1)
        transactions_by_users_report_dyf = DynamicFrame.fromDF(transactions_by_users_report_df, self.glue_context, "transactions_by_users_report_df")
        self._save_output_to_redshift(transactions_by_users_report_dyf, "transactions_by_users_report_df",
                                "s3://s3-bucket-multithreading/transactions_by_users_report/")
		# set to default
        self.sc.setLocalProperty("spark.scheduler.pool", None)
        self.logger.info("Completed account reports..")

    def __create_subscriptions_report(self, selected_df):
        self.logger.info("Starting security reports..")
		#  set pool local to 2 
        self.sc.setLocalProperty("spark.scheduler.pool", str("2"))
        transactions_by_subscription_report_df = selected_df.groupBy('security_id', 'security_name', "date").agg(sum("amount").alias("total")).repartition(1)
        transactions_by_subscription_report_dyf = DynamicFrame.fromDF(transactions_by_subscription_report_df, self.glue_context, "transactions_by_subscription_report_df")
        self._save_output_to_redshift(transactions_by_subscription_report_dyf, "transactions_by_subscription_report_df",
                                "s3://s3-bucket-multithreading/transactions_by_subscription_report/")
								# set to default
        self.logger.info("Completed security reports..")

        # Use write_dynamic_frame to create function that stores Dataframe in Redshift tables

    def _save_output_to_redshift(self, p_data_frame, p_name_of_dyanmic, p_location):
        logger = self.glue_context.get_logger()
        logger.info("Saving Data Frame {} in Redshift at location {}".format(p_name_of_dyanmic, p_location))
        self.glue_context.write_dynamic_frame.from_options(frame=p_data_frame, connection_type="redshift", connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-609272431185-us-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.trial_registers",
        "connectionName": "redshift-crawler",
        "preactions": "CREATE TABLE IF NOT EXISTS public.transactions_by_subscription_report (transaction_id INTEGER, account_id VARCHAR, subscription_id INTEGER, paid_amount INTEGER, register_date VARCHAR, subscription_type VARCHAR, subscribe_newsletter VARCHAR, subscription_name VARCHAR, subscription_register_type VARCHAR, subscription_code INTEGER, subscription_release VARCHAR, subscription_end VARCHAR ),;",
    })
        logger.info("Saved Data in Redshift tables")




    def __start_spark_glue_context(self):
        conf = SparkConf().setAppName("python_thread").set('spark.scheduler.mode', 'FAIR').set("spark.scheduler.allocation.file", self.fair_scheduler_config_file)
        self.sc = SparkContext(conf=conf)
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session


def main():
    job = JobBase()
    job.execute()
    job.__start_spark_glue_context()
    job._save_output_to_s3()
    job.__create_account_report()
    job.__create_security_report()
    
    
    


if __name__ == '__main__':
    main()
