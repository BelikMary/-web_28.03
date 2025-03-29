import json
import pathlib
import csv

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="download_rocket_local",
    description="Download rocket pictures of recently launched rockets.",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
)

# Download launches.json
download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /opt/airflow/data/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/?net__gte=2025-03-01&net__lte=2025-03-31'",  # noqa: E501
    dag=dag,
)

def _get_pictures():
    # Обеспечиваем существование директории для изображений
    images_dir = "/opt/airflow/data/images2"
    pathlib.Path(images_dir).mkdir(parents=True, exist_ok=True)
    error_file = "/opt/airflow/data/error_urls.csv"

    # Загружаем данные из файла JSON
    with open("/opt/airflow/data/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]

    # Открываем CSV-файл и записываем ошибки
    with open(error_file, "w", newline="", encoding="utf-8") as error_log:
        csv_writer = csv.writer(error_log, delimiter=",")
        csv_writer.writerow(["URL", "Message"])  # Заголовки

        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                response.raise_for_status()  # Проверяем ошибки HTTP

                image_filename = image_url.split("/")[-1]
                target_file = f"{images_dir}/{image_filename}"

                with open(target_file, "wb") as img_file:
                    img_file.write(response.content)

                print(f"Downloaded {image_url} to {target_file}")
                csv_writer.writerow([image_url, "Good URL"])
            except requests_exceptions.MissingSchema:
                csv_writer.writerow([image_url, "Invalid URL"])
                print(f"Invalid URL: {image_url}")
            except requests_exceptions.ConnectionError:
                csv_writer.writerow([image_url, "ConnectionError"])
                print(f"Could not connect to {image_url}")
            except requests_exceptions.HTTPError as e:
                csv_writer.writerow([image_url, f"HTTP Error {e.response.status_code}"])
                print(f"HTTP Error {e.response.status_code} for {image_url}")

# Определение задачи в Airflow
get_pictures = PythonOperator(
    task_id="get_pictures", 
    python_callable=_get_pictures, 
    dag=dag
)

# Обновляем команду уведомления, чтобы она считала количество изображений в папке data/images
notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /opt/airflow/data/images/ | wc -l) images."',
    dag=dag,
)

download_launches >> get_pictures >> notify