# نصب عامل Kafdeck روی Kafka با Java 21

فایل `kafdeck-kafka-agent.jar` در بسته، با bytecode نسخهٔ Java 11 ساخته و همین فایل روی Java 21 آزمایش شده است. عامل در همان JVM بروکر اجرا می‌شود؛ JVM را عوض نمی‌کند و JMX remote، exporter یا Prometheus لازم ندارد. نسخهٔ Java بروکر باید در ماتریس پشتیبانی توزیع Kafka خودتان باشد. عامل اصلاح‌شده روی هر سه بروکر واقعی Arad Office با Java 11 نیز بدون restart فعال و دادهٔ واقعی آن بررسی شده است.

## ۱. ثبت منبع

در Kafdeck، کلاستر را انتخاب و **Monitoring → Sources** را باز کنید. برای هر بروکر یک منبع مستقل بسازید و کلید نمایش‌داده‌شده را در فایل `agent-token` روی همان بروکر ذخیره کنید. سه بروکر سه کلید متفاوت دارند. رمز ورود Keycloak یا پسورد Kafka را به‌جای این کلید نگذارید.

## ۲. فایل‌های عامل

JAR و فایل زیر را در `/opt/kafdeck-monitor` داخل محیط اجرای بروکر قرار دهید:

```properties
endpoint=https://kafdeck.company.local:8080/monitoring/ingest
tokenFile=/opt/kafdeck-monitor/agent-token
intervalSeconds=30
includeTopicPartitions=false
# فقط برای CA خصوصی؛ مسیر داخل کانتینر/بروکر:
# caFile=/opt/kafdeck-monitor/ca.pem
```

فایل properties با مسیرهای Linux نوشته شده؛ برای Kafka روی Windows، در مقدار properties از `C:/...` استفاده کنید تا backslash فرمت Java Properties مسیر را تغییر ندهد. endpoint باید از شبکهٔ بروکر قابل دسترسی و certificate آن معتبر باشد؛ redirect دنبال نمی‌شود. روی سرور مقصد هرگز endpoint را `127.0.0.1` لپ‌تاپ فعلی نگذارید.

مالک `agent-token` و `agent.properties` باید حساب اجرای Kafka باشد و فقط همان حساب بتواند آن‌ها را بخواند، مثلاً mode `0600`. CA و JAR نیاز به write برای Kafka ندارند. کلید را در command line، Docker environment یا image bake نکنید. برای کانتینر UID/GID واقعی را با `docker exec kafka-1 id` بررسی کنید.

## ۳. اجرای Docker Compose

برای هر بروکر یک پوشهٔ خصوصی میزبان، مثلاً `/srv/kafdeck-agent/kafka-1` با چهار فایل JAR، properties، token و در صورت نیاز ca.pem بسازید. mount فقط‌خواندنی اضافه کنید و گزینهٔ agent را **به گزینه‌های فعلی JVM اضافه کنید**. مقادیر SSL/SASL/JAAS و agentهای موجود را حذف نکنید:

```yaml
services:
  kafka-1:
    volumes:
      - /srv/kafdeck-agent/kafka-1:/opt/kafdeck-monitor:ro
    environment:
      KAFKA_OPTS: "${KAFKA1_EXISTING_OPTS:-} -javaagent:/opt/kafdeck-monitor/kafdeck-kafka-agent.jar=/opt/kafdeck-monitor/agent.properties"
```

`KAFKA1_EXISTING_OPTS` را برابر گزینه‌های قبلی همین بروکر بگذارید؛ اگر مقدار قبلی خالی بود خالی بماند. برای kafka-2 و kafka-3 mount و کلید مستقل تعریف کنید. فایل‌ها باید برای UID کانتینر قابل خواندن باشند.

نصب عادی `-javaagent` در شروع JVM اعمال می‌شود. در پنجرهٔ نگهداری، پس از بررسی replication و ISR، بروکرها را طبق روال خودتان یکی‌یکی recreate/restart و سلامت هر بروکر را تأیید کنید. روی کلاستری با تاپیک RF=1، rolling restart را بی‌وقفه فرض نکنید. عامل‌های Arad Office در این کار به‌صورت زنده به‌روز شده‌اند؛ برای آن‌ها اکنون restart لازم نیست. helper یک‌بارهٔ مهاجرت زنده بخشی از بستهٔ نصب عمومی نیست.

## ۴. Kafka خارج از Docker

همان `-javaagent:/opt/kafdeck-monitor/kafdeck-kafka-agent.jar=/opt/kafdeck-monitor/agent.properties` را به `KAFKA_OPTS` در launcher یا service environment موجود اضافه کنید و restart نگهداری انجام دهید. سایر گزینه‌ها را حفظ کنید. برای Windows broker نیز از `-javaagent:C:/kafdeck-agent/kafdeck-kafka-agent.jar=C:/kafdeck-agent/agent.properties` استفاده کنید. عامل برای broker/controllerهای جدا باید روی JVM مربوط نصب شود.

## ۵. آزمون پس از نصب

1. لاگ Kafka نباید `Kafdeck monitoring configuration invalid` یا خطای ارسال مداوم نشان دهد. HTTP 204 پذیرش batch است؛ 401 کلید نادرست/باطل، 403 مرز HTTPS، و 429 cadence بیش از حد یا عامل تکراری را نشان می‌دهد.
2. بعد از یک چرخه، منبع در Sources باید Current شود؛ برای GC و رشدها دو نمونه لازم است. CPU، GC، Heap و Produce queue p99 را با زمان تازه بررسی کنید. از Metric explorer نیز یک نام واقعی انتخاب کنید.
3. تنظیم interval از داشبورد به عامل‌ها می‌رسد؛ ۱۰ تا ۶۰ ثانیه پشتیبانی می‌شود. انتخاب Topic از داشبورد حداکثر ۳۲ تاپیک را به عامل می‌دهد. با `includeTopicPartitions=false`، dimensionهای تاپیک تا زمان انتخاب صریح جمع نمی‌شوند؛ متریک‌های aggregate همچنان جمع می‌شوند.
4. Unavailable metric details نام متریک و JMX bean را نشان می‌دهد. برای Java/G1، برخی UsageThresholdها ذاتاً پشتیبانی نمی‌شوند؛ صفر ساختگی ندارند و باعث Partial نمی‌شوند. خطای واقعی getter، ناپدیدشدن bean یا سقف جمع‌آوری همچنان Partial می‌شود.
5. برای تعویض کلید، Rotate همان source را بزنید و فایل token را اتمی جایگزین کنید؛ نیازی به restart Kafka نیست. Revoke داده و اعتبار همان source را حذف می‌کند؛ برای عیب‌یابی معمول آن را نزنید.

حدود هر چرخه: تا ۶۴ packet، هر packet حداکثر ۸۱۹۲ مقدار و کمتر از ۲MB. نام/تعداد متریک‌ها به نسخهٔ Kafka، JVM، workload و replica placement بستگی دارد؛ متریک پشتیبانی‌نشده را نمی‌شود با تغییر UI تولید کرد. لاگ payload پیام‌ها یا credentialها جمع نمی‌شود.
