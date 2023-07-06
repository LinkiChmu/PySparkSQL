from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add

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

df_new_cases_per_week = (df.where(
                                (col('location') != col('continent')) &
                                (col('date').between('2021-03-24', '2021-03-31')))
                         .groupby('location').sum('new_cases')
                         .sort(col('sum(new_cases)').desc())
                         )
df_new_cases_per_week.show(10)

rus_prev = (df.select(
                'date',
                col('new_cases').alias('new_cases_yesterday'))
            .where(
                (col('location').startswith('Rus')) &
                (col('date').between('2021-03-24', '2021-03-30')))
            )

rus_next = (df.select(
                'date',
                col('new_cases').alias('new_cases_today'))
            .where(
                (col('location').startswith('Rus')) &
                (col('date').between('2021-03-25', '2021-03-31')))
            )
rus_diff_new_cases = (
        rus_prev
        .withColumn('date', date_add(rus_prev.date, 1))  # set into the column 'date' next day to join on it
        .join(rus_next, on='date')
        .withColumn('diff', col('new_cases_today') - col('new_cases_yesterday'))
        )
rus_diff_new_cases.show()






