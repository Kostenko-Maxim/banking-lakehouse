# Журнал проверок

Дата: **2026-10-03**, Windows / Docker Desktop + WSL2, amd64.
Компьютер: 15.92 ГиБ RAM, Docker Engine 29.3.1, Compose 5.1.1, память Docker 7.72 ГиБ.
Python хоста 3.12.6, изолированный .venv; контейнер Spark: Python 3.11, Java 17.0.20.1.

| Проверка | Фактический результат |
|---|---|
| Python unit tests | **12 passed**; генератор, manifest, схемы, ограниченный экспорт, DECIMAL, конфигурация |
| Ruff + compileall | пройдены |
| `docker compose config --quiet` | пройден |
| Рабочие Spark/Iceberg-преобразования, jobs/checks.py | **пройдены**, exit code 0, 191.00 RUB / 7.50 USD |
| Raw replay, исправление, отмена, late operation | пройдены в Spark/Iceberg |
| Поздняя история SCD2, привязка фактов | пройдены в Spark/Iceberg |
| Критичная DQ-ошибка, карантин | пройдены в Spark/Iceberg |
| Добавление memo, запрет DECIMAL→STRING, чтение старой схемы | пройдены в Spark/Iceberg |
| PostgreSQL 16.6: COPY → staging → publication | пройдено на настоящем PostgreSQL |
| Откат при сбое, атомарный marker, сверка сумм | пройдены на PostgreSQL; тестовая транзакция целиком отменена после проверки |
| Локальный HTTP webhook, task_failure и freshness | пройдены с настоящим PostgreSQL |
| HDFS 3.3.6: upload, manifest, SHA-256, replay | пройдены на NameNode + DataNode |
| Перезапуск HDFS и сохранность исходных файлов | пройдено, файлы проверялись **до** возможной повторной записи |
| Перезапуск PostgreSQL | сохранность журнала alerts подтверждена сравнением fingerprint |
| Распределённый Spark master + worker + HDFS + Iceberg | **пройдено**, отдельный driver, 20 транзакций в DDS; HadoopCatalog под /warehouse/smoke |
| Полный Airflow → Hive catalog → Trino → PostgreSQL | **не выполнен** |
| Полное `make integration-test` | **не выполнено** |

Для Spark/Iceberg использован настоящий runtime 1.7.1 с embedded HadoopCatalog на локальной
файловой системе контейнера. Это проверка рабочего jobs/pipeline.py, но она не подтверждает
Hive Metastore, Trino или оркестрацию Airflow. Сценарии полного стенда реализованы в `bank.demo`
и `scripts/integration.py`; их наличие не считается результатом успешного прогона.

В первом прогоне Docker Hub вернул timeout. Последующая загрузка PostgreSQL удалась.
Полная сборка остановлена при остатке около 3 ГБ на диске C: и дефиците свободной памяти
хоста. Образы Airflow, Hive и Trino вместе с полным demo в первом прогоне не проверялись.
Финальные Dockerfiles включают уменьшенную сборку Hadoop и COPY --chown для Airflow;
полный комплект финальных образов требует пересборки через `make bootstrap`.

Проверка перезапуска выявила и помогла исправить привязку NameNode RPC к одной из двух сетей:
теперь `dfs.namenode.rpc-bind-host=0.0.0.0`. Spark master получил уникальный DNS alias внутренней
сети. Проброс loopback-портов PostgreSQL и NameNode после добавления interfaces проверен.

Фактический **однократный** sample: PostgreSQL 22.58 MiB, NameNode 188.8 MiB,
DataNode 247.2 MiB (суммарно около 459 MiB). Во время отдельной Spark-проверки наблюдалось
1.057 GiB при лимите 2 GiB. Это не peak RSS и не потребление всего demo-профиля.
Ограничения ресурсов полного стенда пока не подтверждены измерением.

Локальные артефакты (игнорируются Git): `artifacts/spark-checks.log`,
`artifacts/spark-check-state.json`, `artifacts/distributed-smoke.log`, `artifacts/partial-resource-sample.jsonl`,
`artifacts/postgres-before-restart.txt`, `artifacts/postgres-after-restart.txt`.
Команды воспроизведения: README, scripts/spark-checks.sh и make integration-test.

Нагрузочные прогоны 1m и 10m пока не выполнены. Результаты ускорения и требования
к памяти для этих объёмов не заявлены. Команды находятся в benchmark.md.

## Повторный запуск после переноса Docker на диск E:

При запуске полного комплекта образов обнаружены и исправлены ошибки запуска:

- Hive 3.1.3 содержал JDBC-драйвер `postgresql-9.4.1208.jre7.jar` одновременно с новым
  драйвером. Старый драйвер выбирался первым и не поддерживал SCRAM-аутентификацию PostgreSQL 16.
  Сборка удаляет старые JDBC-драйверы перед установкой единственного `postgresql-42.7.4.jar`;
  отдельный Druid-модуль `postgresql-metadata-storage` сохраняется.
- Trino launcher теперь получает `--etc-dir=/etc/trino`. Скрипт сохраняет JVM-флаги образа,
  включая `-Djava.security.manager=allow`, и заменяет только параметры размера heap.
- Airflow webserver использует два процесса Gunicorn вместо четырёх и timeout запуска master
  300 секунд вместо 120. При холодном старте приложения и дефиците RAM проверка readiness
  может занимать несколько минут.

Подтверждено на настоящих сервисах: создание схемы Hive (`SCHEMA_VERSION=3.1.0`), healthy Metastore,
запрос Trino `SHOW SCHEMAS FROM iceberg` через Hive catalog, healthy Airflow scheduler.
Все десять постоянных сервисов достигли healthy; Airflow `/health`, Trino `/v1/info`
и alerts `/health` возвращают HTTP 200 с Windows. Airflow сообщает healthy для metadatabase
и scheduler; Trino сообщает `starting=false`.
Повторные Python-тесты: **12 passed**, Ruff и Compose config прошли.
Эти проверки запуска сервисов не заменяют полный прогон demo и integration-test.
