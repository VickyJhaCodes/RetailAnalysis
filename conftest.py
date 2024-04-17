import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
    "Creates and stops Spark Session"
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop() 

@pytest.fixture
def expected_results(spark):
    schema="state string,count int"
    filepath="data/test_results/state_aggregated.csv"
    return spark.read \
        .format("csv") \
        .schema(schema) \
        .load(filepath)