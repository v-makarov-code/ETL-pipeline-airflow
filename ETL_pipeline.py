import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import pandahouse as ph


default_args = {
    'owner': 'v.makarov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 21),
}


@dag(default_args=default_args, catchup=False, schedule_interval='0 12 * * *')
def vladislav_makarov_bxs7496_DAG():
    @task()
    def get_feed_actions():
        # Запрос
        query = """
        SELECT user_id,
               os,
               gender, 
               age,
               countIf(action='view') AS views,
               countIf(action='like') AS likes
          FROM simulator_20250520.feed_actions
         GROUP BY user_id, os, gender, age
        """
        # Подключение
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'simulator_20250520',
                      'user':'student',
                  'password':'dpo_python_2020'
                     }
        # Результат запроса 
        df = ph.read_clickhouse(query, connection=connection)
        return df
    @task()
    def get_message_actions():
        # Запрос 
        query = """
            SELECT 
                users.user_id AS user_id,
                users.os AS os, 
                users.age AS age,
                users.gender AS gender,
                received.messages_received AS messages_received,
                sent.messages_sent AS messages_sent,
                received.users_received AS users_received,
                sent.users_sent AS users_sent
             FROM
                (
            --Подзапрос с данными о пользователе
            SELECT DISTINCT 
                        user_id,
                        os,
                        age,
                        gender
              FROM simulator_20250520.message_actions
                ) AS users
            LEFT JOIN
                (
            --Подзапрос сколько пишет, скольким людям пишет
            SELECT
                t1.user_id AS user_id,
                COUNT() AS messages_sent,
                COUNT(DISTINCT t1.receiver_id) AS users_sent
             FROM simulator_20250520.message_actions AS t1
            GROUP BY t1.user_id
                ) AS sent
             ON sent.user_id = users.user_id
           LEFT JOIN
                (
            --Подзапрос сколько получает, скольку людей пишут ему
            SELECT
                t2.receiver_id AS user_id,
                COUNT() AS messages_received,
                COUNT(DISTINCT t2.user_id) AS users_received
             FROM simulator_20250520.message_actions AS t2
            GROUP BY t2.receiver_id
                ) AS received
              ON received.user_id = users.user_id 
                """
        # Подключение
        connection = {'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'simulator_20250520',
                      'user':'student',
                  'password':'dpo_python_2020'
                     }
        # Результат запроса 
        df = ph.read_clickhouse(query, connection=connection)
        return df
    @task()
    def join_tables(df1, df2):
        new_df = pd.merge(df1, df2, on=['user_id', 'os', 'gender', 'age'], how='outer')
        new_df.fillna(0, inplace=True)
        return new_df
    @task()
    def calc_os_metrics(df):
        os_df = df.groupby("os", as_index=False).agg({"views":"sum",
                                              "likes":"sum",
                                              "messages_received":"sum",
                                              "messages_sent":"sum", 
                                              "users_received":"sum", 
                                              "users_sent":"sum"})
        os_df.rename(columns={"os": "dimension_value"}, inplace=True)
        os_df.insert(loc=0, column="dimension", value="os")
        os_df = os_df.astype({"dimension":"str",
                               "dimension_value":"str",
                               "views":"uint64", 
                               "likes":"uint64",
                               "messages_received":"uint64",
                               "messages_sent":"uint64", 
                               "users_received":"uint64",
                               "users_sent":"uint64"})
        
        return os_df
    @task()
    def calc_gender_metrics(df):
        gender_df = df.groupby("gender", as_index=False).agg({"views":"sum",
                                              "likes":"sum",
                                              "messages_received":"sum",
                                              "messages_sent":"sum", 
                                              "users_received":"sum", 
                                              "users_sent":"sum"})
        gender_df.rename(columns={"gender": "dimension_value"}, inplace=True)
        gender_df.insert(loc=0, column="dimension", value="gender")
        gender_df = gender_df.astype({"dimension":"str",
                               "dimension_value":"str",
                               "views":"uint64", 
                               "likes":"uint64",
                               "messages_received":"uint64",
                               "messages_sent":"uint64", 
                               "users_received":"uint64",
                               "users_sent":"uint64"})
        return gender_df
    @task()
    def calc_age_metrics(df):
        age_df = df.groupby("age", as_index=False).agg({"views":"sum",
                                              "likes":"sum",
                                              "messages_received":"sum",
                                              "messages_sent":"sum", 
                                              "users_received":"sum", 
                                              "users_sent":"sum"})
        age_df.rename(columns={"age": "dimension_value"}, inplace=True)
        age_df.insert(loc=0, column="dimension", value="age")
        age_df = age_df.astype({"dimension":"str",
                               "dimension_value":"str",
                               "views":"uint64", 
                               "likes":"uint64",
                               "messages_received":"uint64",
                               "messages_sent":"uint64", 
                               "users_received":"uint64",
                               "users_sent":"uint64"})
        
        return age_df
    @task()
    def update_table(os_df, gender_df, age_df):
        # Подключение test 
        connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                           'database':'test',
                           'user':'student-rw', 
                           'password':'656e2b0c9c'
                           }
        #Запрос
        query_test = '''CREATE TABLE IF NOT EXISTS test.vladislav_makarov_bxs7496
                                (event_date Date,
                                 dimension String, 
                                 dimension_value String, 
                                 views UInt64, 
                                 likes UInt64, 
                                 messages_received UInt64,
                                 messages_sent UInt64,
                                 users_received UInt64,
                                 users_sent UInt64    
                                )
                        ENGINE = MergeTree()
                        ORDER BY event_date'''
        # Создаем таблицу если еще не создана
        ph.execute(query_test, connection=connection_test)
        
        # Добаляем текущую дату для каждого датафрейма
        context = get_current_context()
        ds = context['ds']
        os_df.insert(loc=0, column="event_date", value=pd.to_datetime(ds))
        gender_df.insert(loc=0, column="event_date", value=pd.to_datetime(ds))
        age_df.insert(loc=0, column="event_date", value=pd.to_datetime(ds))
        
        # Обновляем таблицу
        ph.to_clickhouse(
            df=os_df, table="vladislav_makarov_bxs7496", index=False, connection=connection_test)
        ph.to_clickhouse(
            df=gender_df, table="vladislav_makarov_bxs7496", index=False, connection=connection_test)
        ph.to_clickhouse(
            df=age_df, table="vladislav_makarov_bxs7496", index=False, connection=connection_test)
        
    #Exctract
    feed_actions = get_feed_actions()
    message_actions = get_message_actions()
    #Transform
    metrics = join_tables(feed_actions, message_actions)
    os_df = calc_os_metrics(metrics)
    gender_df = calc_gender_metrics(metrics)
    age_df = calc_age_metrics(metrics)
    #Load
    update_table(os_df, gender_df, age_df)
    
vladislav_makarov_bxs7496_DAG = vladislav_makarov_bxs7496_DAG()
    
        
        
        


        
        
        
        
        
    
        
        
        
        
        
        
        
        
    