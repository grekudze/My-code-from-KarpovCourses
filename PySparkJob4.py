import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def process(spark, flights_path, airlines_path, airports_path, result_path):
    """
    Основной процесс задачи.

    :param spark: SparkSession
    :param flights_path: путь до датасета c рейсами
    :param airlines_path: путь до датасета c авиалиниями
    :param airports_path: путь до датасета c аэропортами
    :param result_path: путь с результатами преобразований
    """
    airlines_df=spark.read.parquet(airlines_path)
    airports_df=spark.read.parquet(airports_path)
    flights_df=spark.read.parquet(flights_path)

    airlines_df=airlines_df.withColumnRenamed('AIRLINE','AIRLINE_NAME')
    joined_df = airlines_df.join(flights_df, airlines_df.IATA_CODE == flights_df.AIRLINE, how='inner')
    joined_df_2=joined_df.join(airports_df,joined_df.ORIGIN_AIRPORT==airports_df.IATA_CODE,how='inner')
    joined_df_2=joined_df_2.withColumnRenamed('AIRPORT','ORIGIN_AIRPORT_NAME')\
    .withColumnRenamed('LATITUDE','ORIGIN_LATITUDE')\
    .withColumnRenamed('LONGITUDE','ORIGIN_LONGITUDE').withColumnRenamed('COUNTRY','ORIGIN_COUNTRY')
    airports_df=airports_df.withColumnRenamed('IATA_CODE','IATA_CODE_1')
    joined_df_3=joined_df_2.join(airports_df,joined_df_2.DESTINATION_AIRPORT==airports_df.IATA_CODE_1,how='inner')
    joined_df_3=joined_df_3.withColumnRenamed('AIRPORT','DESTINATION_AIRPORT_NAME').withColumnRenamed('LATITUDE','DESTINATION_LATITUDE')\
    .withColumnRenamed('LONGITUDE','DESTINATION_LONGITUDE').withColumnRenamed('COUNTRY','DESTINATION_COUNTRY')
    last_df=joined_df_3.select('AIRLINE_NAME','TAIL_NUMBER','ORIGIN_COUNTRY','ORIGIN_AIRPORT_NAME','ORIGIN_LATITUDE','ORIGIN_LONGITUDE','DESTINATION_COUNTRY','DESTINATION_AIRPORT_NAME','DESTINATION_LATITUDE','DESTINATION_LONGITUDE')

    last_df.write.parquet(result_path, mode='overwrite')


def main(flights_path, airlines_path, airports_path, result_path):
    spark = _spark_session()
    process(spark, flights_path, airlines_path, airports_path, result_path)


def _spark_session():
    """
    Создание SparkSession.

    :return: SparkSession
    """
    return SparkSession.builder.appName('PySparkJob4').getOrCreate()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--flights_path', type=str, default='flights.parquet', help='Please set flights datasets path.')
    parser.add_argument('--airlines_path', type=str, default='airlines.parquet', help='Please set airlines datasets path.')
    parser.add_argument('--airports_path', type=str, default='airports.parquet', help='Please set airports datasets path.')
    parser.add_argument('--result_path', type=str, default='result', help='Please set result path.')
    args = parser.parse_args()
    flights_path = args.flights_path
    airlines_path = args.airlines_path
    airports_path = args.airports_path
    result_path = args.result_path
    main(flights_path, airlines_path, airports_path, result_path)
