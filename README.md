# Инструкция к установке psql

## Обновление репозиториев, добавление репозитория docker и установка инструментария docker и docker compose
```
sudo apt update
sudo apt-get install ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

## Разрешаем порт 5432 (порт postgre)
```
sudo ufw allow 5432
```

## Перед установкой, отредактируйте данные в docker-compose.yml в строке POSTGRES_PASSWORD
```
cd install

docker compose up -d
```

## Настройка postgresql
```
docker ps

(скопировать CONTAINER ID например 78b56e48e06f)

docker exec -it 78b56e48e06f psql -U postgres psql

CREATE USER your_user WITH PASSWORD 'yourPassword';

CREATE DATABASE db_name;

GRANT CONNECT ON DATABASE db_name TO your_user;

ctrl+d
```

## Подготовка к запуску python-приложения

```
sudo apt install python3
sudo apt install python3-pip
sudo pip install psycopg2

(Если возникают ошибки с либой libpq-dev)
sudo -H pip3 install --upgrade pip
sudo pip install --upgrade wheel
sudo pip install --upgrade setuptools
sudo apt-get install --reinstall libpq-dev
```

## Запуск python-приложения (из директории install)

```
sudo python3 ./test.py &
```
## В случае ответа от сервера 404, 401 и тд:
```
Необходимо на машине, на которой скрипт заведомо работает и ответ от сервера дает 200, произвести команду ping или traceroute
Например: 

ping wickads-ld.irev.com
traceroute wickads-ld.irev.com

Полученный ip адрес необходимо приравнять на хосте(где ошибка) к нужному нам домену(wickads-ld.irev.com) в /etc/hosts:

nano /etc/hosts
(вписываем в таком формате)
<полученный айпи> wickads-ld.irev.com
ctrl+x
enter
