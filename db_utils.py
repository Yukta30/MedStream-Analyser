import boto3
import yaml

def save_to_redshift(df, db_config, table_name):
    """
    Save DataFrame to Amazon Redshift.
    """
    jdbc_url = db_config['redshift']['jdbc_url']
    temp_dir = db_config['redshift']['tempdir']

    # Write dataframe to Redshift using Spark JDBC
    df.write \
        .format("com.databricks.spark.redshift") \
        .option("url", jdbc_url) \
        .option("user", db_config['redshift']['user']) \
        .option("password", db_config['redshift']['password']) \
        .option("dbtable", table_name) \
        .option("tempdir", temp_dir) \
        .mode("append") \
        .save()
