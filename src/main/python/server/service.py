import os
import httpx
from typing import Any
import pyspark.pandas as ps
from smolagents import CodeAgent
from smolagents import OpenAIServerModel
from mcp.server.fastmcp import FastMCP
from tools import smol_spark
from base import TmpContext
from data import Data
from app import main


agent = CodeAgent(
	    tools = [smol_spark.spark_sql],
        model = OpenAIServerModel(
            model_id=os.environ.get("LOCAL_MODEL"),
            api_base=os.environ.get("OPENAI_API_BASE"),
            api_key=os.environ.get("OPENAI_API_KEY"),
    	),
	)


@main.mcp.tool()
async def sql_query(query: str) -> str:
    """Run a Spark SQL query
    
    Args:
        query: query to run
    """
    ctx = TmpContext()
    data = Data(ctx)
    sample = data.sample(scale=2.0)
    pdf = sample["sample_table"]
    df = ps.from_pandas(pdf).to_spark()
    df.createOrReplaceTempView("sample_table")
    response = agent.run(query)
    return response

