import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, sum, when, col


def process(spark, flights_path, airlines_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param result_path: путь с результатами преобразований
    """
    airlines_df = spark.read.parquet(airlines_path)
    flights_df = spark.read.parquet(flights_path)

    airlines_df = airlines_df.withColumnRenamed('AIRLINE', 'AIRLINE_NAME')

    joined_df = airlines_df.join(flights_df, airlines_df.IATA_CODE == flights_df.AIRLINE, how='inner')

    joined_df.groupBy('AIRLINE_NAME') \
        .agg(count(when(col('CANCELLED') == 0, 1)).alias('correct_count'),
             sum(when(col('DIVERTED') == 1, 1).otherwise(0)).alias('diverted_count'),
             sum(when(col('CANCELLED') == 1, 1).otherwise(0)).alias('cancelled_count'),
             avg('DISTANCE').alias('avg_distance'),
             avg('AIR_TIME').alias('avg_air_time'),
             count(when(col('CANCELLATION_REASON') == 'A', 1)).alias('airline_issue_count'),
             count(when(col('CANCELLATION_REASON') == 'B', 1)).alias('weather_issue_count'),
             count(when(col('CANCELLATION_REASON') == 'C', 1)).alias('nas_issue_count'),
             count(when(col('CANCELLATION_REASON') == 'D', 1)).alias('security_issue_count')) \
        .orderBy('AIRLINE_NAME') \
        .write.parquet(result_path, mode='overwrite')


def main(flights_path, airlines_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob5').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    result_path = args.result_path
    main(flights_path, airlines_path, result_path)

