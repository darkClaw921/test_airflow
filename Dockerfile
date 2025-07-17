FROM apache/airflow:3.0.1

# Переключаемся на root для установки системных пакетов
USER root

# Устанавливаем системные зависимости
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         libmariadb-dev \
         default-libmysqlclient-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Копируем файлы проекта
COPY requirements.txt /opt/airflow/
COPY app/ /opt/airflow/app/
COPY dags/ /opt/airflow/dags/
# COPY plugins/ /opt/airflow/plugins/
COPY config/ /opt/airflow/config/

# Переключаемся на пользователя airflow для установки Python-пакетов
USER airflow

# Устанавливаем зависимости с помощью pip
RUN pip install --no-cache-dir "apache-airflow==3.0.1" \
    && pip install --no-cache-dir -r /opt/airflow/requirements.txt \
    && pip install --no-cache-dir loguru

# Устанавливаем переменные окружения
ENV PYTHONPATH=/opt/airflow

# Настраиваем директории с правами на запись
RUN mkdir -p /opt/airflow/logs/dag_processor \
    && mkdir -p /opt/airflow/logs/scheduler \
    && mkdir -p /opt/airflow/logs/webserver \
    && mkdir -p /opt/airflow/logs/worker

# Запускаем Airflow в режиме standalone
CMD ["airflow", "standalone"] 