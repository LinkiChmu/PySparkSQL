from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, lag

"""Using data set 'covid-data.csv':
1.Select the 15 countries with the highest percentage of recoveries as of March 31, 2021 
(columns: iso_code, country, percentage of recoveries).
2.Top 10 countries with the highest reported number of new cases in the last week of March 2021 
in descending sorted order (columns: date, country, number of new cases).
3. Calculate the change in cases relative to the previous day in Russia for the last week of March 2021.
For example: in Russia yesterday there were 9150 , today 8763, difference: -387 
(columns: date, number of new cases yesterday, number of new cases today, difference)
"""
spark = (SparkSession.builder
         .appName('PySparkSQL')
         .master('local[*]')
         .getOrCreate()
         )
df = (spark.read
      .option('header', True)
      .option('inferschema', True)
      .csv('covid-data.csv')
      )

df_recoveries_percentage = (df.select(
                                'iso_code',
                                col('location').alias('country'),
                                (col('total_cases') / col('population') * 100).alias('recoveries_percentage'))
                            .where(col('date') == '2021-03-31')
                            .sort(col('recoveries_percentage').desc())
                            )
df_recoveries_percentage.show(15)

df_mart_last_week = (df.select(
                        'date',
                        col('location').alias('country'),
                        'new_cases')
                     .where((col('date').between('2021-03-25', '2021-03-31')) &
                            (col('location') != col('continent'))))

window = Window.partitionBy('country').orderBy(col('new_cases').desc())

df_mart_last_week = (df_mart_last_week
                     .withColumn('row', row_number().over(window))
                     .where(col('row') == 1)
                     .drop('row')
                     .sort(col('new_cases').desc())
                     )
df_mart_last_week.show(10)

df_rus = (df.select(
                'date',
                col('new_cases').alias('new_cases_today'))
            .where(
                (col('location').startswith('Rus')) &
                (col('date').between('2021-03-24', '2021-03-31'))))

window_spec = Window.partitionBy().orderBy('date')
rus_diff = (df_rus
            .withColumn('new_cases_yesterday', lag(col('new_cases_today')).over(window_spec))
            .withColumn('diff', col('new_cases_today') - col('new_cases_yesterday')))
rus_diff.show()
