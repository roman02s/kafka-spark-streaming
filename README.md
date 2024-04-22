# Интеграция Apache Kafka и Spark Streaming: инференс в онлайн пайплайнах ML


## Команда
- Одобеску Роман
- Сим Роман


# Описание



kafka-topics --list --bootstrap-server localhost:29092
kafka-topics --create --topic house-data --bootstrap-server localhost:29092
kafka-console-producer --topic house-data --bootstrap-server localhost:29092
kafka-console-consumer --topic house-data-predictions --bootstrap-server localhost:29092

