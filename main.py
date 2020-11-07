from pyspark.sql import SparkSession
from faker import Faker
import random

fake = Faker()
spark = SparkSession.builder.appName("PipelineBuilder").getOrCreate()
sc = spark.sparkContext


def forename():
    return fake.name().split(" ")[0]


def surname():
    return fake.name().split(" ")[1]


def birth_date():
    return fake.date_between(start_date='-100y', end_date='today')


def generate_dataframe():
    return spark.createDataFrame([(i, forename(), surname(), birth_date()) for i in range(0, 100)],
                                 ['id', 'forename', 'surname', 'birth_date'])


def main():
    df = generate_dataframe()
    df.show()


if __name__ == '__main__':
    main()

