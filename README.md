# Kafka schema registry + connect + elasticsearch example

## Инструкции
Для того, чтобы подключить коннектор elasticsearch необходимо его скачать коннектор версии 14.1.2
с хаба confluent https://www.confluent.io/hub/confluentinc/kafka-connect-elasticsearch

После скачивания, разместите все имеющиеся jar в папке ./docker/kafka-connect-jars
Затем запустите кластер кафки+schema-registry+connect

Для того, чтобы создать коннектор к elasticsearch необходимо вызвать rest api kafka-connect.
В папке http уже имеются готовые запросы:
1. create_connector.http - создание коннектора с нужными параметрами
2. task_status.http - проверка статуса работы коннектора