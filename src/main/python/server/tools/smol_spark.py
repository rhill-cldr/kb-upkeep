from smolagents import tool as smol_tool
from pyspark.sql import Row, DataFrame, SparkSession


@smol_tool
def spark_sql(sql_query: str) -> str:
    """
    Allows you to perform Spark SQL queries. Returns a markdown table format representation of the result.
    Try to always include a LIMIT with your queries, and execute DESCRIBE queries or similar if necessary.
    
    Args:
        sql_query: The Spark SQL query to perform. This should be correct SQL for Spark version 3.5.4.
    """
    spark = SparkSession.builder \
        .config("spark.sql.ansi.enabled", False) \
        .config("spark.sql.debug.maxToStringFields", 100000).getOrCreate()
    result = spark.sql(sql_query)
    if not result:
        return ""
    else:
        return result.toPandas().to_markdown()


