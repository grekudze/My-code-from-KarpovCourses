import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import count, avg, desc

def process(spark, flights_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param result_path: путь с результатами преобразований
    """
    flight_df = spark.read.parquet(flights_path)

    route_stats = flight_df.groupBy('ORIGIN_AIRPORT', 'DESTINATION_AIRPORT') \
                      .agg(count('TAIL_NUMBER').alias('tail_count'), avg('AIR_TIME').alias('avg_air_time')) \
                      .orderBy(desc('tail_count')) \
                      .limit(10)

    route_stats.write.parquet(result_path, mode='overwrite')

                      


def main(flights_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob1').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    result_path = args.result_path
    main(flights_path, result_path)






Copy code
from pyspark.sql.functions import avg, min, max, corr

orig_air = flight_df.groupBy('ORIGIN_AIRPORT')\
    .agg(avg('DEPARTURE_DELAY').alias('avg_delay'),
         min('DEPARTURE_DELAY').alias('min_delay'),
         max('DEPARTURE_DELAY').alias('max_delay'),
         corr('DEPARTURE_DELAY', 'DAY_OF_WEEK').alias('corr_delay2day_of_week'))\
    .select('ORIGIN_AIRPORT', 'avg_delay', 'min_delay', 'max_delay', 'corr_delay2day_of_week')\
    .filter('max_delay >= 1000')
