# Инфраструктура студента

Здесь разместите конфигурации выбранных сервисов; основную топологию — в корневом compose.yaml/stack.yaml. `compose.example.yaml` — только worksheet ролей и намеренно не запускается. `contracts/infrastructure.json` задаёт минимальные роли, не готовые образы/пароли.

В REPORT.md сопоставьте роль → service name → версия/digest → health/readiness → volume → сеть/порт → способ передачи credentials. Приведите bootstrap (schema, bucket, broker topology, а с lab6 provisioning) и безопасное повторение этих команд.

Приложение строится из исходников. Не используйте заранее существующий образ преподавательского сервера вместо своей реализации. Restart hook перезапускает все соответствующие stores с volumes; failover hook останавливает только заданную API-реплику.
