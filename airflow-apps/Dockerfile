FROM ubuntu
RUN apt update -y
RUN apt upgrade -y
RUN apt install python3 -y
RUN apt install pip -y
RUN apt install git -y
RUN apt install curl -y
RUN apt-get install libmysqlclient-dev -y
ARG DEBIAN_FRONTEND=noninteractive
RUN apt install libpq-dev -y

# Retrieve packages from Microsoft
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Update packages for the image
RUN apt-get update

# Install sql driver and tools
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev

# Install MS SQL Tools
RUN ACCEPT_EULA=Y apt-get install -y mssql-tools

# Adds paths to the $PATH environment variable within the .bash_profile and .bashrc files
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN echo 'export PYTHONPATH="/root/source/airflow-apps"' >> ~/.bash_profile
RUN echo 'export PYTHONPATH="/root/source/airflow-apps"' >> ~/.bashrc
RUN echo 'export POSTGRES_DEFS="/root/source/postgres"' >> ~/.bash_profile
RUN echo 'export POSTGRES_DEFS="/root/source/postgres"' >> ~/.bashrc
ENV PYTHONPATH=/root/source/airflow-apps
ENV POSTGRES_DEFS=/root/source/postgres
RUN pip install apache-airflow[snowflake,postgres,amazon,microsoft-mssql,mysql]==2.4
RUN apt install supervisor -y
ADD supervisord.conf /etc/supervisor/conf.d/supervisord.conf
ENV TZ=America/New_York
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=en_US.utf8
RUN apt install postgresql -y
ENV POSTGRES_HOST_AUTH_METHOD=trust
#WORKDIR /root/.pip
#ADD pip.conf /root/.pip/pip.conf
RUN apt install git
RUN apt-get install nano
WORKDIR /source
WORKDIR /root
RUN mkdir ~/airflow
WORKDIR /root/airflow
COPY airflow.cfg /root/airflow/airflow.cfg
COPY requirements.txt /root/airflow/requirements.txt
RUN pip install -r /root/airflow/requirements.txt
RUN pg_lsclusters
USER postgres
RUN pg_ctlcluster 14 main start \
    && pg_lsclusters \ 
    && psql -c "CREATE USER airflow SUPERUSER PASSWORD 'airflow'" \
    && psql -c "CREATE DATABASE airflow;"
USER root
RUN pg_ctlcluster 14 main start \
    && pg_lsclusters \ 
    && airflow db init \
    && airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
EXPOSE 8080
ENTRYPOINT pg_ctlcluster 14 main start \
    && /usr/bin/supervisord -n