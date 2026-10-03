# Воспроизводимый эксперимент

Нагрузочные измерения запускаются отдельно; основной demo использует несколько операций.

```bash
make benchmark PROFILE=small REPEATS=3
make benchmark PROFILE=1m REPEATS=3
make benchmark PROFILE=10m REPEATS=3
docker compose exec -T airflow-scheduler ls /data/benchmarks
docker compose cp airflow-scheduler:/data/benchmarks artifacts/benchmarks
docker compose stats --no-stream > artifacts/docker-stats.txt
docker info > artifacts/docker-info.txt
```

При нагрузке остановите airflow-webserver, если UI не нужен; scheduler нужен для spark-submit.
Изменения ресурсов фиксируйте вместе с результатом. Дополнительный worker не обязателен:
основной эксперимент использует один worker и два executor cores.

| Вариант | Shuffle | Join | Таблица | Target file size |
|---|---:|---|---|---:|
| baseline | 64 | shuffle join, auto broadcast отключён | без partition transform | 16 MiB |
| candidate | 8 | явный broadcast небольшого accounts | days(event_at) | 128 MiB |

В обоих вариантах AQE отключён для сравнимости заданного shuffle; исходники и суммы одинаковые.
Порядок чередуется между повторениями. Cold cache не гарантируется: это отражено в JSON.
Однодневный источник не показывает выигрыш partition pruning на многодневном диапазоне;
не экстраполируйте этот результат на него. Изменяются несколько факторов сразу: этот тест
оценивает комбинацию; для причинного вывода повторите эксперимент, меняя по одному фактору.

Сохраняются: платформа контейнера, доступные CPU, /proc/meminfo, cgroup limit, batch ID,
число повторов, настройки, времена записи/чтения и filtered query, количество и суммарный
размер файлов, физический план, суммы и количество строк, shuffle read/write bytes.
Байты shuffle читаются из реального Spark event log по job group. JSON не заполняет
отсутствующие метрики выдуманными значениями; отсутствие нужных событий вызывает ошибку.

Spark event logs находятся в именованном volume spark-events. Сохраните их вместе с JSON:
`docker compose cp airflow-scheduler:/opt/spark-events artifacts/spark-events`.
Генерация и upload не входят в сравниваемое время выполнения.

Фактический статус измерений — в validation.md. Если результата нет, ускорение не доказано.
