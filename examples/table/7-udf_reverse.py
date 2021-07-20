from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.expressions import col
from pyflink.table.udf import udf, ScalarFunction

# Encapsulate UDF within its own class
class ReverseString(ScalarFunction):
  def eval(self, string):
    return string[::-1]

# Register UDF
reverse = udf(ReverseString(), result_type=DataTypes.STRING())

# Configure table environment
settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = TableEnvironment.create(settings)
t_env.get_config().get_configuration().set_string("parallelism.default", "1")

# Create source and sink
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

# Define and execute a query operation that uses the `reverse` UDF
t_env.from_path('mySource') \
    .group_by(col('word')) \
    .select(reverse(col('word')), col('word').count) \
    .insert_into('mySink') \

t_env.execute("7-udf_reverse")
