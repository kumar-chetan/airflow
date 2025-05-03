import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from datetime import datetime,timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fetch API Key from environment variables
API_KEY = os.getenv('OPENWEATHER_API_KEY')

# Define the locations for weather extraction
locations = {
    'Najafgarh': {'lat': 28.6096, 'lon': 76.9798},
    'Dwarka': {'lat': 28.5945, 'lon': 77.0460}
}



def send_email_smtp(**context):
    from_email = os.getenv("EMAIL_ADDRESS")  # e.g., your Gmail
    password = os.getenv("EMAIL_PASSWORD")   # App password (not your real Gmail password)
    to_email = "9013chetankumar@gmail.com"

    html_content = context['ti'].xcom_pull(task_ids='compose_email_content')

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = "Today's Weather Report for Delhi"
    msg.attach(MIMEText(html_content, 'html'))

    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.starttls()
        server.login(from_email, password)
        server.send_message(msg)


def compose_email_content(**context):
    import pandas as pd

    output_dir = '/opt/airflow/processed_data'
    csv_file = f'{output_dir}/processed_wether.csv'

    if not os.path.exists(csv_file):
        return "<p>No processed data found.</p>"

    df = pd.read_csv(csv_file, header=None, names=['location', 'temp', 'humidity', 'condition'])
    
    content = "<h3>Today's Weather Report:</h3><table border='1' style='border-collapse: collapse;'>"
    content += "<tr><th>Location</th><th>Temperature (°C)</th><th>Humidity (%)</th><th>Condition</th></tr>"

    for _, row in df.iterrows():
        content += f"<tr><td>{row['location']}</td><td>{row['temp']}</td><td>{row['humidity']}</td><td>{row['condition'].title()}</td></tr>"

    content += "</table>"

    return content




def store_data():
    hook = PostgresHook(postgres_conn_id='postgres')
    file_path = '/opt/airflow/processed_data/processed_wether.csv'
    hook.copy_expert(
        sql="COPY weather FROM stdin WITH DELIMITER as ','",
        filename=file_path
    )

def extract_weather_data():
    weather_data = {}

    for location, coords in locations.items():
        # API call to fetch weather data
        response = requests.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params={
                'lat': coords['lat'],
                'lon': coords['lon'],
                'appid': API_KEY,
                'units': 'metric'
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"Data for {location}: {data}")  # Log the full response for debugging
            if 'main' in data:
                weather_data[location] = {
                    'temp': data['main']['temp'],
                    'humidity': data['main']['humidity'],
                    'condition': data['weather'][0]['description'].title()
                }
            else:
                print(f"Missing 'main' data for {location}")
        else:
            print(f"Failed to fetch weather for {location}: {response.status_code} - {response.text}")
    
    return weather_data

def process_wether_data(ti):
    import os
    import pandas as pd
    
    # Ensure the directory exists
    output_dir = '/opt/airflow/processed_data'
    os.makedirs(output_dir, exist_ok=True)
    weather_data=ti.xcom_pull(task_ids="extract_weather_data")
    normalized_data = [
    {'location': location, **details}
    for location, details in weather_data.items()
    ]
    df = pd.DataFrame(normalized_data)

    df.to_csv(f'{output_dir}/processed_wether.csv', index=False, header=False)

    



# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'weather_data_extraction',
    default_args=default_args,
    description='A simple DAG to extract weather data',
    schedule_interval='@daily',  # Set to run daily
    start_date=datetime(2025, 5, 3),
    catchup=False,
)

# Create a task to extract weather data
extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    dag=dag,
)

process_task=PythonOperator(
    task_id="process_task",
    python_callable=process_wether_data
)

create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS weather (
                location TEXT NOT NULL,
                temerature TEXT NOT NULL,
                humidity TEXT NOT NULL,
                condition TEXT NOT NULL
                
            );
        '''
    )

store_wether_data = PythonOperator(
        task_id='store_weather_data',
        python_callable=store_data
    )

compose_email = PythonOperator(
        task_id='compose_email_content',
        python_callable=compose_email_content,
        provide_context=True
    )

send_email = PythonOperator(
    task_id='send_weather_email',
    python_callable=send_email_smtp,
    provide_context=True
)


# Set the task dependencies (here we only have one task)
extract_task >> process_task >> create_table >> store_wether_data >> compose_email >> send_email

