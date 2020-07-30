FROM php:7.2-cli

RUN apt-get update -y

RUN apt-get install -y libpq-dev
RUN docker-php-ext-install pdo_mysql pdo_pgsql

WORKDIR /app

CMD ["tail", "-f", "/dev/null"]