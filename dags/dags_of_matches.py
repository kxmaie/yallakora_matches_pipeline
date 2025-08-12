from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import snowflake.connector
from datetime import datetime,timedelta
import pendulum
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os


dbt_project_path = "/usr/local/airflow/dags/dbt_matches"

def fetch_data():
    date=datetime.today().strftime('%m/%d/%Y')
    page=requests.get(f"https://www.yallakora.com/match-center?date={date}#days")
    def main_fun(page):
        src=page.content
        soup=BeautifulSoup(src,"lxml")
        match_list=[]
        champoinchips=soup.find_all("div",{'class':'matchCard'})
        for matches in champoinchips:
            title=matches.find('h2').get_text(strip=True)
            matche=matches.find_all("div",attrs={"livescorematchid":True})
            for i in matche:
                 info_of_match=i.find("div",class_='date').get_text(strip=True)
                 status_of_match=i.find("div",class_='matchStatus').get_text(strip=True)
                 team_A=i.find("div",class_="teamA").get_text(strip=True)
                 team_B=i.find("div",class_="teamB").get_text(strip=True)
                 result=i.find("div",class_="MResult").find_all("span",class_='score',limit=2)
                 if (len(result)==2):
                     score=f'"{result[0].get_text(strip=True)} - {result[1].get_text(strip=True)}"'

                 else:
                     score="-"
                 time=i.find("div",class_='MResult').find("span",class_='time').get_text(strip=True)

                 match_list.append({
                     'tournament':title,
                     'match_info':info_of_match,
                     'status':status_of_match,
                     'team_ateam_a':team_A,
                     ' team_b':team_B,
                     'score':score,
                     'time':time

                 })

        new_data = pd.DataFrame(match_list)

        file_path = '/usr/local/airflow/include/matches.csv'

        if os.path.exists(file_path):
           old_data = pd.read_csv(file_path)
           combined_data = pd.concat([old_data, new_data], ignore_index=True)
        else:
          combined_data = new_data

        combined_data.to_csv(file_path, encoding='utf-8-sig', index=False)
        
    main_fun(page)

def fetch_played_matches():
    date=(datetime.today()-timedelta(days=1)).strftime('%m/%d/%y')
    page=requests.get(f"https://www.yallakora.com/match-center?date={date}#days")
    def main_fun(page):
        src=page.content
        soup=BeautifulSoup(src,"lxml")
        match_list=[]
        champoinchips=soup.find_all("div",{'class':'matchCard'})
        for matches in champoinchips:
            title=matches.find('h2').get_text(strip=True)
            matche=matches.find_all("div",attrs={"livescorematchid":True})
            for i in matche:
                 info_of_match=i.find("div",class_='date').get_text(strip=True)
                 status_of_match=i.find("div",class_='matchStatus').get_text(strip=True)
                 team_A=i.find("div",class_="teamA").get_text(strip=True)
                 team_B=i.find("div",class_="teamB").get_text(strip=True)
                 result=i.find("div",class_="MResult").find_all("span",class_='score',limit=2)
                 if (len(result)==2):
                     score=f'"{result[0].get_text(strip=True)} - {result[1].get_text(strip=True)}"'

                 else:
                     score="-"
                 time=i.find("div",class_='MResult').find("span",class_='time').get_text(strip=True)

                 match_list.append({
                     'tournament':title,
                     'match_info':info_of_match,
                     'status':status_of_match,
                     'team_ateam_a':team_A,
                     ' team_b':team_B,
                     'score':score,
                     'time':time

                 })

        new_data = pd.DataFrame(match_list)

        file_path = '/usr/local/airflow/include/played_matches.csv'

        if os.path.exists(file_path):
           old_data = pd.read_csv(file_path)
           combined_data = pd.concat([old_data, new_data], ignore_index=True)
        else:
          combined_data = new_data

        combined_data.to_csv(file_path, encoding='utf-8-sig', index=False)
        
    main_fun(page)


def check_snowflake_objects_and_load():
    conn = snowflake.connector.connect(
        user='DBT_MO',
        password='dpt_password',
        account='bl16247.me-central2.gcp',
        warehouse='INFO_OF_MATCHES',
        database='INFO_MATCHES_DB',
        schema='RAW',
        role='ACCOUNTADMIN'
    )
    cs = conn.cursor()

    try:
       
        cs.execute("PUT file:///usr/local/airflow/include/played_matches.csv @MATCHES_STAGE/played_matches.csv OVERWRITE=TRUE")
        cs.execute("PUT file:///usr/local/airflow/include/matches.csv @MATCHES_STAGE/matches.csv OVERWRITE=TRUE")

        cs.execute(''' 
      COPY INTO RAW.PLAYED_MATCHES
      FROM @MATCHES_STAGE/played_matches.csv
      FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)



''')
        cs.execute('''
            COPY INTO RAW.MATCHES
            FROM @MATCHES_STAGE/matches.csv
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'CONTINUE';
        ''')
        print("✅ تم رفع البيانات بنجاح إلى Snowflake.")

    except Exception as e:
        print(f"❌ حدث خطأ أثناء الاتصال أو التنفيذ: {e}")
    finally:
        cs.close()
        conn.close()


local_tz='Africa/Cairo'
with DAG(dag_id='fetch_matches_info',
         description='this dag is to fitch data from yalla kora and load it in snowflake and using dbt',
         start_date=pendulum.datetime(2025,8,5,tz=local_tz),
         schedule='0 0 * * *',
         catchup=False,
         dagrun_timeout=timedelta(minutes=45)) as dag:
    
    
    task_of_fetch_data=PythonOperator(
        task_id='fetch_save_data',
        python_callable=fetch_data


    )

    task_of_fetch_played_matches=PythonOperator(
        task_id='fetch_played_matches',
        python_callable=fetch_played_matches
    )

    task_of_load_data=PythonOperator(
        task_id='load_data',
        python_callable=check_snowflake_objects_and_load
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models_with_bash",
        bash_command=f"cd {dbt_project_path} && dbt run --profiles-dir {dbt_project_path}",
    )
  



task_of_fetch_data >> task_of_fetch_played_matches>> task_of_load_data >> run_dbt_models


