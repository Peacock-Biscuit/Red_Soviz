# Red Soviz

Baseball analysts can use MLB Data API to analyze performances of baseball players. Normally, when we go to MLB website, we couldn't quickly compare performance because it is just tabular data. Therefore, the project is to faciliate analytics process for professional analysts, especially for Red Sox. 

## Installation
<!-- <h3 align="left">Languages and Tools</h3> -->
<p align="left"> 
  <a href="https://airflow.apache.org/" target="_blank">
    <img src="https://airflow.apache.org/docs/apache-airflow/stable/_images/pin_large.png" alt="Airflow" width="40" height="40"/>
  </a>
  <a href="https://www.python.org" target="_blank"> 
    <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> 
  </a>
  <a href="https://www.mysql.com/" target="_blank"> 
    <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/mysql/mysql-original-wordmark.svg" alt="mysql" width="40" height="40"/> 
  </a> 
</p>
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install apache-airflow, pandas and sqlalchemy.

```bash
pip install apache-airflow
pip install pandas
pip install sqlalchemy
```
## Usage

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

dag = DAG(
    dag_id="red_soviz",
    description="Red Sox 40 man STAT data.",
    start_date=airflow.utils.dates.days_ago(5),
    schedule_interval="@weekly",
)
# Pseudocode
def get_player():
    url = "path"
    get_data = requests.request("GET", url)

def get_hitting():
    return

def get_pitching():
    return

get_players = PythonOperator(
    task_id="get_player",
    python_callable=get_player,
    dag=dag
)

get_40men_hitting = PythonOperator(
    task_id="get_hitting",
    python_callable=get_hitting,
    dag=dag
)

get_40men_pitching = PythonOperator(
    task_id="get_pitching",
    python_callable=get_pitching,
    dag=dag
)
# dependency
get_players >> [get_40men_hitting, get_40men_pitching]

```
![Airflow Workflow](https://github.com/cyliu657/Red_Soviz/blob/main/images/workflow.png "Airflow Workflow")

