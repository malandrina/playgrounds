from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.expressions import col
from pyflink.table.udf import udf, ScalarFunction

reverse_string = udf(lambda w: w[::-1], [DataTypes.STRING()], DataTypes.STRING())

settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = TableEnvironment.create(settings)
t_env.get_config().get_configuration().set_string("parallelism.default", "1")

t_env.execute_sql("""
        CREATE TABLE mySource (
          word STRING
        ) WITH (
          'connector' = 'filesystem',
          'format' = 'csv',
          'path' = '/opt/examples/table/input/udf_reverse_input'
        )
    """)

t_env.execute_sql("""
        CREATE TABLE mySink (
          `reversed_word` STRING,
          `count` BIGINT
        ) WITH (
          'connector' = 'filesystem',
          'format' = 'csv',
          'path' = '/opt/examples/table/output/udf_reverse_output'
        )
    """)

t_env.from_path('mySource') \
    .group_by(col('word')) \
    .select(reverse_string(col('word')), col('word').count) \
    .insert_into('mySink') \

t_env.execute("7-udf_reverse")
