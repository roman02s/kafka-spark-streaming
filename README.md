# Интеграция Apache Kafka и Spark Streaming: инференс в онлайн пайплайнах ML

## Команда
- Одобеску Роман
- Сим Роман

## Описание
В репозитории представлен код для интеграции Apache Kafka и Spark Streaming на примере задачи предсказания цены дома (2 ДЗ). Таким образом, data generator пишет табличные данные в один из топиков в Kafka, который постоянно мониторит Spark Streaming. Затем через Spark Pipeline данные преобразуются и обрабатываются моделью. Как итог, финальные предсказания пишутся в другой топик, из которого они могут попасть в хранилище или другой сервис.

## Список команд

Собираем контейнеры:
```bash
docker compose build
```
Запускаем контейнеры:
```bash
docker compose up
```

Список топиков:
```bash
kafka-topics --list --bootstrap-server localhost:29092
```

Создание топика:
```bash
kafka-topics --create --topic house-data --bootstrap-server localhost:29092
```

Вход в producer консоль топика, из которой можно добавить новые записи в топик:
```bash
kafka-console-producer --topic house-data --bootstrap-server localhost:29092
```

Вход в consumer консоль топика, из которой можно увидеть новые записи в топике:
```bash
kafka-console-consumer --topic house-data-predictions --bootstrap-server localhost:29092
```

## [Презентация](https://docs.google.com/presentation/d/1w3ynKwz-rAIKhUm7sPUcqwu2-JFyjuoG/edit#slide=id.p1)

## [Демо](https://drive.google.com/file/d/1KhWaJ0HDl7M8sQaQEoU55delaob0a_SS/view?usp=sharing)
