


import sys

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.context import SparkConf
from pyspark.sql.functions import *
from awsglue.transforms import Join, SelectFields


###############################
class JobBase(object):


    # We should take it from configuration property
    fair_scheduler_config_file= "fairscheduler.xml"

    def execute(self):

        self.__start_spark_glue_context()

        args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME'])
        self.logger = self.glue_context.get_logger()
        self.logger.info("Starting Glue Threading job ")
		
		# reading of catalog table
        transactions_df = self.glue_context.create_dynamic_frame.from_catalog(database="glue_s3_test", table_name="transactions",
                                                                              redshift_tmp_dir=args["TempDir"],
                                                                              transformation_ctx="datasource0")
        account_df = self.glue_context.create_dynamic_frame.from_catalog(database="glue_s3_test", table_name="account",
                                                                         redshift_tmp_dir=args["TempDir"],
                                                                         transformation_ctx="datasource0")

        securities_df = self.glue_context.create_dynamic_frame.from_catalog(database="glue_s3_test", table_name="securities",
                                                                            redshift_tmp_dir=args["TempDir"],
                                                                            transformation_ctx="datasource0")

		# joining
        transactions_df = transactions_df.rename_field('acount_id', 'trn_acount_id').rename_field('security_id', 'trn_security_id')
        securities_df = securities_df.rename_field('name', 'security_name')
        joined_df= Join.apply(Join.apply(transactions_df, account_df, 'acount_id', 'trn_acount_id'), securities_df, 'security_id', 'trn_security_id')
        selected_df = SelectFields.apply(frame = joined_df, paths = ['transaction_id', 'account_id', 'security_id', "date","amount", "security_name","holder_name"] ).toDF()
		selected_df.cache()
        import concurrent.futures
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
		# calling method using Python thread
        executor.submit(self.__create_security_report,sc=selected_df)
        executor.submit(self.__create_account_report,sc=selected_df)
        self.logger.info("Completed Threading job")

    def __create_account_report(self, selected_df):
        self.logger.info("Starting account reports..")
		#  set pool local to 1
        self.sc.setLocalProperty("spark.scheduler.pool", str("1"))
        acc_by_sell_report_df = selected_df.groupBy('account_id', 'holder_name', "date").agg(sum("amount").alias("total")).repartition(1)
        acc_by_sell_report_dyf = DynamicFrame.fromDF(acc_by_sell_report_df, self.glue_context, "acc_by_sell_report_df")
        self._save_output_to_s3(acc_by_sell_report_dyf, "acc_by_sell_report_df",
                                "s3://s3_bucket/acc_by_sell_report")
		# Better to set it to default
		self.sc.setLocalProperty("spark.scheduler.pool", None)
        self.logger.info("Completed account reports..")

    def __create_security_report(self, selected_df):
        self.logger.info("Starting security reports..")
		#  set pool local to 2 
        self.sc.setLocalProperty("spark.scheduler.pool", str("2"))
        secu_by_sell_report_df = selected_df.groupBy('security_id', 'security_name', "date").agg(sum("amount").alias("total")).repartition(1)
        secu_by_sell_report_dyf = DynamicFrame.fromDF(secu_by_sell_report_df, self.glue_context, "secu_by_sell_report_df")
        self._save_output_to_s3(secu_by_sell_report_dyf, "secu_by_sell_report_df",
                                "s3://s3_bucket/acc_by_sell_report")
								# Better to set it to default
        self.logger.info("Completed security reports..")

    def _save_output_to_s3(self, p_data_frame, p_name_of_dyanmic, p_location):
        logger = self.glue_context.get_logger()
        logger.info("Saving Data Frame {} in S3 at location {}".format(p_name_of_dyanmic, p_location))
        self.glue_context.write_dynamic_frame.from_options(frame=p_data_frame, connection_type="s3", connection_options={"path": p_location}, format="csv")
        logger.info("Saved Data in S3 at location")



    def __start_spark_glue_context(self):
        conf = SparkConf().setAppName("python_thread").set('spark.scheduler.mode', 'FAIR').set("spark.scheduler.allocation.file", self.fair_scheduler_config_file)
        self.sc = SparkContext(conf=conf)
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session


def main():
    job = JobBase()
    job.execute()


if __name__ == '__main__':
    main()
view rawGlue_Python_Thread.py hosted with ❤ by GitHub