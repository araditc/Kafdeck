<div dir="rtl">

**این راهنما برای نصب دستی با زیرساخت موجود است.** برای سرور خالی و بستهٔ شامل Java، PostgreSQL و Keycloak با سه Windows Service خودکار، [راهنمای نصب آفلاین](windows-offline-install.fa.md) را دنبال کنید؛ بخش Scheduled Task این سند مربوط به مسیر دستی قدیمی است.


# نصب بستهٔ مانیتورینگ Kafdeck روی Windows Server

نسخهٔ جاری **۱.۱.۰** است. برای نصب و ارتقای بستهٔ کامل از [راهنمای نصب آفلاین](windows-offline-install.fa.md) و [ارتقای تک‌اسکریپتی](windows-offline-upgrade.fa.md) استفاده کنید. دستورالعمل‌های build آزمایشی در ادامه برای سابقهٔ آن بسته نگه داشته شده‌اند.

این بستهٔ مستقل `win-x64` شامل API، داشبورد و runtime دات‌نت است؛ نصب SDK، Node.js یا Prometheus روی سرور لازم نیست. این یک build آزمایشی مانیتورینگ از مسیر `1.0.0-rc.1` است؛ جایگزین امضاشدهٔ انتشار رسمی ۰.۹ نیست. فایل `build-manifest.json` و `SHA256SUMS` هویت فایل‌های همین build را مشخص می‌کنند.

پیش‌نیازها: Windows Server پشتیبانی‌شدهٔ x64، گواهی HTTPS معتبر برای نام/IP سرور، یک Keycloak با realm اختصاصی، و دسترسی شبکهٔ سرور به issuer و آدرس‌های advertised بروکرهای Kafka. این publish روی ویندوز محلی آزمایش شده است؛ نصب روی Windows Server مقصد را با مراحل زیر بررسی کنید. [سیستم‌عامل‌ها و پیش‌نیازهای دات‌نت](https://learn.microsoft.com/en-us/dotnet/core/install/windows)

## ۱. انتقال فایل‌ها

هش ZIP را با فایل `.sha256` تحویل‌شده مقایسه و بسته را در مسیر دائمی استخراج کنید؛ مثال:

<div dir="ltr">

```powershell
Get-FileHash .\kafdeck-monitoring-win-x64.zip -Algorithm SHA256
Expand-Archive .\kafdeck-monitoring-win-x64.zip C:\Kafdeck
Set-Location C:\Kafdeck
```

</div>

ساختار بسته `app`، `scripts`، `agent` و `docs` است. اسکریپت مقداردهی اولیه، پوشه‌های خصوصی `config`، `secrets` و `state` را می‌سازد. حساب ویندوز اجرای برنامه را از ابتدا مشخص کنید و تمام مراحل را با همان حساب انجام دهید؛ رمز PFX با DPAPI همان حساب و دستگاه محافظت می‌شود. حساب/پسورد ادمین و داده‌های Arad Office داخل ZIP نیستند.

## ۲. آماده‌سازی Keycloak

اگر Keycloak سازمانی دارید از همان سرویس با یک realm اختصاصی مثل `kafdeck` استفاده کنید. در نصب جدا، Java 21 و توزیع رسمی Keycloak و دیتابیس پایدار آن را طبق [راهنمای رسمی](https://www.keycloak.org/server/configuration) آماده کنید. `start-dev` برای استقرار اصلی مناسب نیست. نمونهٔ این مسیر با Keycloak 26.8.1 کار کرده است. Keycloak داخل ZIP برنامه نیست.

در realm اختصاصی:

1. **User registration** و **Verify email** را خاموش کنید. تا قبل از تنظیم SMTP، Verify Email یا بازیابی مبتنی بر ایمیل را required action نکنید.
2. یک client محرمانه به نام `kafdeck-browser`، با Client authentication روشن، Standard Flow روشن و PKCE S256 بسازید. Direct Access Grants را لازم نداریم.
3. با فرض آدرس `https://kafdeck.company.local:8080`، callback ورود را دقیقاً `https://kafdeck.company.local:8080/signin-oidc` و callback خروج را `https://kafdeck.company.local:8080/signout-callback-oidc` ثبت کنید؛ return origin خروج را هم ثبت کنید. wildcard عمومی نگذارید.
4. client محرمانهٔ جدا به نام `kafdeck-user-management`، با Service accounts فعال بسازید. به service account فقط نقش realm-management / `manage-users` در همین realm بدهید؛ `manage-realm` لازم نیست.
5. در User Profile، ویژگی اختیاری و single-valued به نام `kafdeckProvisioningId` اضافه کنید. View و Edit فقط `admin` باشند؛ این ویژگی را به کاربر یا token mapper نمایش ندهید. این مورد برای ساخت و بازیابی امن کاربران عادی لازم است.
6. issuer مانند `https://idp.company.local/realms/kafdeck` باید برای مرورگر و سرور قابل دسترسی و قابل اعتماد باشد. realm `master` پشتیبانی نمی‌شود. دو client secret را جداگانه نگه دارید.

## ۳. تنظیم نصب

فایل PFX باید private key و زنجیرهٔ گواهی مناسب داشته باشد و SAN آن نام/IP واقعی داشبورد را پوشش دهد. دستور زیر مثال است؛ issuer، نام سرور، مسیر گواهی و CIDR را برای محیط خود تغییر دهید:

<div dir="ltr">

```powershell
.\scripts\Initialize-Kafdeck.ps1 `
  -PublicOrigin https://kafdeck.company.local:8080 `
  -KeycloakIssuer https://idp.company.local/realms/kafdeck `
  -CertificateFile C:\Certificates\kafdeck.pfx `
  -RegistrationNetworks '192.168.100.0/24'
```

</div>

اسکریپت دو client secret و رمز PFX را با ورودی مخفی می‌گیرد. تنظیمات در `config\appsettings.Production.json` و state در `state` قرار می‌گیرد. دسترسی این سه پوشه به حساب نصب، SYSTEM و Administrators محدود می‌شود. `RegistrationNetworks` شبکه‌های مجاز ثبت Kafka و سرویس‌های همان کلاستر است؛ آن را به شبکه‌های واقعی محدود کنید. CIDR به‌تنهایی جای egress firewall میزبان را نمی‌گیرد؛ advertised address بروکرها هم باید در مرز شبکهٔ آزموده‌شده باشند.

برای CA خصوصی issuer، پارامتر `-IssuerCaFile C:\Certificates\idp-ca.pem` را هم بدهید. مرورگر باید CA را از مسیر معمول سازمانی بشناسد. این گزینه اعتبارسنجی TLS را خاموش نمی‌کند.

اگر firewall سرور بسته است، فقط ترافیک لازم به پورت 8080 را از شبکه‌های کاربران/بروکرهای مجاز باز کنید؛ مثال زیر را برای محیط خود محدود کنید:

<div dir="ltr">

```powershell
New-NetFirewallRule -DisplayName 'Kafdeck HTTPS' -Direction Inbound `
  -Action Allow -Protocol TCP -LocalPort 8080 -RemoteAddress 192.168.100.0/24
```

</div>

## ۴. ساخت ادمین اول و اجرا

با همان حساب نصب، کد یک‌بارمصرف بسازید و برنامه را اجرا کنید:

<div dir="ltr">

```powershell
.\scripts\Start-Kafdeck.ps1 -IssueSetupCode
.\scripts\Start-Kafdeck.ps1
```

</div>

اولین دستور فقط کد را چاپ می‌کند و listener دوم ایجاد نمی‌کند. کد ۱۵ دقیقه اعتبار دارد. آدرس HTTPS تنظیم‌شده را باز کنید، کد، ایمیل و پسورد ادمین اول را بدهید؛ پسورد ۱۲ تا ۱۲۸ کاراکتر و مطابق policy realm باشد. تأیید ایمیل لازم نیست. سپس از طریق Keycloak وارد شوید. پس از ساخته‌شدن owner، setup بسته می‌شود و تولید کد جدید مجاز نیست. برای کاربر عادی از **Manage users** استفاده کنید؛ پسورد موقت در اولین ورود عوض می‌شود. owner کافدک با ادمین سرور Keycloak یکی نیست.

برای کد منقضی، فقط پیش از ساخته‌شدن owner، دستور IssueSetupCode را دوباره اجرا کنید. برای خطای provisioning یا وضعیت نامعلوم، state را پاک نکنید؛ نتیجه را در realm و لاگ/روند بازیابی بررسی کنید.

بررسی اولیه:

<div dir="ltr">

```powershell
Invoke-WebRequest https://kafdeck.company.local:8080/healthz -UseBasicParsing
```

</div>

پاسخ 200 سلامت listener را ثابت می‌کند؛ ورود، Kafka و agent را جدا با مراحل بعد بررسی کنید. برای اجرای foreground، همین پنجره باز می‌ماند و خطاهای سرور دیده می‌شوند.

## ۵. افزودن کلاستر و فعال‌کردن مانیتورینگ

از **Kafka connections** نام اتصال، bootstrap servers و Security Protocol را بدهید؛ برای SASL/TLS فیلدهای مربوط را کامل کنید. **Test connection** سپس ذخیره/بازکردن را بزنید. کلاستر باید در selector بالا دیده شود و Topics باز شود. Schema Registry، Kafka Connect و ksqlDB آدرس و احراز هویت جدا دارند و از صفحات مربوط تنظیم می‌شوند.

در **Monitoring → Sources** برای هر بروکر منبع جدا با نامی مثل `broker-1` ثبت کنید. کلید هر منبع فقط یک بار نمایش داده می‌شود؛ همان را فقط روی بروکر متناظر قرار دهید. [راهنمای نصب عامل Java 21](kafka-agent-java21.fa.md) را دنبال کنید. `127.0.0.1` ویندوز مقصد و `127.0.0.1` داخل کانتینر Kafka یک میزبان نیستند؛ endpoint عامل باید DNS/IP قابل دسترسی ویندوزسرور باشد.

برای جمع‌آوری metadata در اتصال PLAINTEXT، شناسهٔ کلاستر ذخیره‌شده را از پارامتر `cluster` آدرس داشبورد بردارید، در آرایهٔ `Kafdeck.Monitoring.MetadataClusters` فایل تنظیمات بگذارید و فقط Kafdeck را ری‌استارت کنید. مثال مقدار `connection-...` است؛ نام نمایشی کلاستر نیست. برای اتصالات SASL/TLS این sampler فعلی را فعال نکنید؛ agent داخل JVM مستقل از آن متریک‌های بروکر را می‌فرستد. lag از مسیر احراز هویت همان اتصال Kafka خوانده می‌شود.

در Monitoring تب‌های Overview، Brokers، Topics and partitions، Consumers، Alerts، Metric explorer و Sources در دسترس‌اند. Sampling interval را روی ۱۰، ۳۰ یا ۶۰ ثانیه بگذارید. برای موضوع دلخواه از لیست انتخاب و **Monitor this topic** را بزنید؛ به عامل تا دو چرخه فرصت بدهید. CPU/GC/Request queue و جزئیات خطاها را بررسی کنید. تاریخچهٔ محلی متریک‌ها ۲۴ ساعت است؛ حجم state را با تعداد سری‌ها و cadence پایش کنید.

## ۶. شروع خودکار پس از بوت

پس از آزمون موفق foreground، آن را با Ctrl+C متوقف کنید. PowerShell را با همان حساب نصب و مجوز ثبت Task باز کنید:

<div dir="ltr">

```powershell
.\scripts\Install-StartupTask.ps1
Start-ScheduledTask -TaskName Kafdeck
Get-ScheduledTaskInfo -TaskName Kafdeck
```

</div>

این اسکریپت یک **Scheduled Task** با شروع هنگام بوت، اجرای غیرتعاملی و تلاش مجدد پس از شکست می‌سازد. Windows Service مستقیم نیست؛ باینری را با `sc.exe create` ثبت نکنید. credential خواسته‌شده باید همان حسابی باشد که Initialize را اجرا کرده تا DPAPI و Data Protection ثابت بمانند. حساب باید اجازهٔ Log on as a batch job و دسترسی شبکهٔ لازم داشته باشد. Task موجود بازنویسی نمی‌شود. برای ارتقا ابتدا `Stop-ScheduledTask -TaskName Kafdeck` را اجرا کنید و توقف پردازش را بررسی کنید.

## ۷. ارتقا و عیب‌یابی

قبل از ارتقا، از `config`، `secrets`، کل `state`، realm/دیتابیس Keycloak و کلیدهای Data Protection حساب اجرا، نسخهٔ سازگار و پشتیبان قابل بازیابی تهیه کنید. نویسندهٔ قبلی را متوقف و فقط فایل‌های `app` و اسکریپت‌ها را جایگزین کنید؛ Initialize را روی نصب موجود دوباره اجرا نکنید. rollback به binary و backup متناظر نیاز دارد. کپی فایل DPAPI روی سرور/حساب دیگر به‌تنهایی مهاجرت معتبر نیست.

| وضعیت | بررسی |
| --- | --- |
| صفحه باز نمی‌شود | foreground را اجرا و خطای startup، پورت، firewall و SAN گواهی را بررسی کنید |
| healthz خوب ولی UI نیست | وجود `app\wwwroot\index.html` و اجرای برنامه از Start-Kafdeck را بررسی کنید |
| ورود/ساخت ادمین شکست می‌خورد | issuer، دو client secret جدا، callback دقیق، اعتماد CA و manage-users را بررسی کنید |
| ساخت کاربر عادی بسته است | User Profile / kafdeckProvisioningId با دسترسی admin-only را بررسی کنید |
| broker تازه ثبت نمی‌شود | CIDR مجاز، advertised listeners، احراز هویت Kafka و egress واقعی را بررسی کنید |
| agent داده نمی‌دهد | endpoint از داخل کانتینر، کلید همان source، مالکیت فایل‌ها، CA و لاگ Kafka را بررسی کنید |
| Unsupported by JVM | جزئیات Sources را باز کنید؛ آستانه‌های حافظهٔ پشتیبانی‌نشده خطای جمع‌آوری نیستند |
| No committed offsets | consumer هنوز offset commit نکرده؛ Kafdeck برای نمایش نمودار offset را تغییر نمی‌دهد |

</div>
