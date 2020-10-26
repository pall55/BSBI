from pyspark.sql import SparkSession
from pyspark.sql import functions


class Inverter:
    def __init__(self):
        pass

    @staticmethod
    def create_session(app_name):
        spark_session = SparkSession.builder.master("local[1]").appName(app_name).getOrCreate()
        return spark_session

    @staticmethod
    def invert_index(term_doc_df):
        """
                Method inverts indices as defined in bsbi algorithm using spark
        :param term_doc_df:
        :return:
        """
        spark_session = Inverter.create_session('invert_index')
        spark_term_doc_df = spark_session.createDataFrame(term_doc_df)
        output = spark_term_doc_df.groupby('termID').agg(functions.collect_list('docID').alias('docID_temp'))
        return output.select("*", functions.sort_array(output["docID_temp"]).alias('docID'))
