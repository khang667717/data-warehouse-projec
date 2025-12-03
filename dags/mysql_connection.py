"""
MySQL connection setup for Airflow
"""
from airflow.models import Variable

# These would be set in Airflow UI or as environment variables
MYSQL_CONNECTION = {
    'conn_id': 'mysql_dw',
    'conn_type': 'mysql',
    'host': Variable.get('mysql_host', default_var='mysql-dw'),
    'port': Variable.get('mysql_port', default_var=3306),
    'login': Variable.get('mysql_user', default_var='etl_user'),
    'password': Variable.get('mysql_password', default_var='etl_password'),
    'schema': Variable.get('mysql_dw_db', default_var='sales_dw')
}