FROM python:3.10.5-buster

ENV MYSQL_HOST=value
ENV MYSQL_USER=value
ENV MYSQL_PASSWORD=value
ENV MYSQL_PORT=value
ENV CH_HOST=value
ENV MONGO_HOST=value
ENV MONGO_PORT=value
ENV MONGO_USER=value
ENV MONGO_PASSWORD=value
ENV MONGO_PORT=value

RUN apt-get update -y && apt install gnupg2  wget unzip mariadb-client  iputils-ping -y \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4 

RUN apt-get update -y \
    && wget https://codeload.github.com/datacharmer/test_db/zip/refs/heads/master --output-document=data.zip && unzip data.zip\
    && wget https://codeload.github.com/neelabalan/mongodb-sample-dataset/zip/refs/heads/main --output-document=mongodb-sample-dataset.zip \
    && wget https://downloads.mysql.com/docs/sakila-db.zip --output-document=sakila-db.zip && unzip sakila-db.zip \
    && cp sakila-db/* test_db-master/

WORKDIR /test_db-master
COPY execs.sh execs.sh
CMD ["bash","execs.sh"]
