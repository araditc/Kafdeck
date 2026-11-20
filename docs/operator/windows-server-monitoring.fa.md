<div dir="rtl">

# نصب Kafdeck از صفر روی Windows Server

نسخهٔ جاری **۱.۱.۰** است. برای نصب و ارتقای بستهٔ کامل از [راهنمای نصب آفلاین](windows-offline-install.fa.md) و [ارتقای تک‌اسکریپتی](windows-offline-upgrade.fa.md) استفاده کنید. دستورالعمل‌های build آزمایشی در ادامه برای سابقهٔ آن بسته نگه داشته شده‌اند.

این راهنما برای **یک سرور خالی** است؛ Keycloak سازمانی لازم نیست. برنامه و سیستم ورود را روی همین ویندوز نصب می‌کنیم. Kafka روی سرورهای خودش باقی می‌ماند.

در پایان، داشبورد در `https://SERVER-IP:8080` و سیستم ورود در `https://SERVER-IP:8443` باز می‌شوند. SERVER-IP یعنی آی‌پی ثابت ویندوزسرور شما.

سه دانلود جانبی داریم: **Java 21، PostgreSQL 17 و Keycloak**. نصب .NET، Node.js، Docker یا Prometheus روی این ویندوز لازم نیست. این بسته build آزمایشی مانیتورینگ است. راهنما برای Windows Server 2019/2022 با معماری x64 است؛ خودکارسازی روی ویندوز محلی بررسی شده و روی سرور مقصد نیز باید مراحل پایانی را بررسی کنید.

## ۱. بسته را استخراج کنید

فایل `kafdeck-monitoring-win-x64.zip` را روی سرور ببرید. راست‌کلیک → **Extract All** → مسیر `C:\Kafdeck`.

داخل این پوشه باید مستقیماً `app`، `scripts`، `docs` و `agent` را ببینید. از اول تا آخر نصب با **همان حساب ویندوز** کار کنید.

## ۲. Java را نصب کنید

[صفحهٔ رسمی Java مایکروسافت](https://learn.microsoft.com/en-us/java/openjdk/download#openjdk-21) را باز کنید. از بخش **OpenJDK 21**، گزینهٔ **Windows / x64 / MSI** را بگیرید. نصب‌کننده را اجرا و Next → Install → Finish را بزنید. گزینهٔ **Set JAVA_HOME** را، اگر نمایش داده شد، فعال کنید. Java بروکرهای Kafka را تغییر ندهید؛ این Java برای ویندوز است.

## ۳. PostgreSQL را نصب کنید

[دانلود PostgreSQL برای ویندوز](https://www.postgresql.org/download/windows/) → **Download the installer** → **PostgreSQL 17 / Windows x86-64**.

نصب‌کننده را اجرا کنید. مسیر پیش‌فرض، **PostgreSQL Server** و **Command Line Tools** را نگه دارید. pgAdmin اختیاری است. برای کاربر `postgres` یک رمز قوی انتخاب و یادداشت کنید. پورت را **5432** بگذارید. نصب Stack Builder لازم نیست.

پورت 5432 را برای شبکه باز نکنید؛ فقط همین ویندوز به دیتابیس متصل می‌شود. اگر PostgreSQL از قبل دارید، نصب دوم انجام ندهید؛ از [راهنمای تکمیلی](windows-server-monitoring-advanced.fa.md) استفاده کنید.

## ۴. Keycloak و تنظیمات ورود را آماده کنید

[ZIP رسمی Keycloak 26.8.1](https://github.com/keycloak/keycloak/releases/download/26.8.1/keycloak-26.8.1.zip) را بگیرید و در `C:\Keycloak` استخراج کنید. این فایل باید وجود داشته باشد:

`C:\Keycloak\keycloak-26.8.1\bin\kc.bat`

از Start، **Windows PowerShell** را پیدا و با راست‌کلیک **Run as administrator** را بزنید. کد زیر را کامل کپی کنید. آی‌پی ویندوزسرور و میزبان‌های Kafka را می‌پرسد؛ پورت ننویسید. برای Arad Office فعلی، پاسخ سؤال دوم فقط `192.168.100.33` است.

<div dir="ltr">

```powershell
Set-Location C:\Kafdeck
$ServerIP = Read-Host 'Windows Server IPv4 address'
$KafkaIPs = (Read-Host 'Kafka host IPv4 addresses, separated by comma').Split(',')
$Networks = @($KafkaIPs | ForEach-Object { $_.Trim() + '/32' })
.\scripts\Prepare-NewServer.ps1 -ServerAddress $ServerIP `
  -KafkaNetworks $Networks `
  -KeycloakHome C:\Keycloak\keycloak-26.8.1
```

</div>

اسکریپت، گواهی مخصوص همین سرور، تنظیمات ورود، اتصال دو برنامه و فایل ساخت دیتابیس را می‌سازد. **Realm و Clientها را دستی نمی‌سازید.** ثبت‌نام عمومی و تأیید ایمیل خاموش‌اند. ادمین یا پسورد ثابت داخل بسته نیست.

اگر خطای JAVA_HOME گرفتید، در همان پنجره این خط را اجرا و دستور Prepare را تکرار کنید:

<div dir="ltr">

```powershell
$env:JAVA_HOME = (Get-ChildItem 'C:\Program Files\Microsoft\jdk-21*' -Directory | Select-Object -First 1).FullName
```

</div>

اگر ویندوز فایل دانلودشده را بسته است، منبع ZIP را بررسی کنید و در **Properties** خود ZIP گزینهٔ **Unblock** را بزنید؛ سپس دوباره استخراج کنید. اگر پیام «running scripts is disabled» دارید و محدودیت از Group Policy سازمانی نیست، فقط در همان پنجره دستور زیر را اجرا کنید و دوباره ادامه دهید. این تنظیم با بستن پنجره از بین می‌رود. برای Group Policy سازمانی از مسئول ویندوز بخواهید همین فایل‌ها را بررسی و مجاز کند. [توضیح مایکروسافت](https://learn.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_execution_policies?view=powershell-5.1)

<div dir="ltr">

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
```

</div>

## ۵. دیتابیس و گواهی را ثبت کنید

در همان PowerShell اجرا کنید:

<div dir="ltr">

```powershell
& 'C:\Program Files\PostgreSQL\17\bin\psql.exe' -X -h 127.0.0.1 -U postgres -d postgres -f C:\Kafdeck\secrets\create-keycloak-db.sql
Import-Certificate -FilePath C:\Kafdeck\secrets\server-public.cer -CertStoreLocation Cert:\LocalMachine\Root
```

</div>

دستور اول رمز `postgres` مرحلهٔ ۳ را می‌خواهد؛ هنگام تایپ چیزی دیده نمی‌شود. باید **CREATE ROLE** و **CREATE DATABASE** ببینید. این مرحله را برای نصب تازه **یک بار** اجرا کنید.

دستور دوم گواهی همین نصب را به ویندوز معرفی می‌کند. اثر انگشت آن در پایان مرحلهٔ ۴ چاپ شده است. فقط گواهی همان نصب خودتان را اعتماد کنید. این گواهی داخلی یک سال اعتبار دارد.

برای بازکردن داشبورد روی کامپیوتر دیگر، **فقط فایل عمومی** `server-public.cer` را به آن ببرید و همان دستور Import-Certificate را با مسیر فایل عمومی، در PowerShell با دسترسی Administrator اجرا کنید. پوشهٔ secrets و فایل‌های PFX، DPAPI و SQL را به کاربران ندهید. برای انتشار اینترنتی یا گواهی سازمانی، [راهنمای تکمیلی](windows-server-monitoring-advanced.fa.md) را ببینید.

## ۶. سیستم ورود را روشن کنید

<div dir="ltr">

```powershell
.\scripts\Start-Keycloak.ps1 -BootstrapAdmin
```

</div>

یک رمز حداقل ۱۲ کاراکتری برای مدیر موقت Keycloak انتخاب کنید؛ نام کاربری او `bootstrap-admin` است. این حساب برای نگهداری سیستم ورود است؛ ادمین Kafdeck را در مرحلهٔ بعد می‌سازید.

اولین اجرا ممکن است چند دقیقه طول بکشد. پس از پیام شروع Keycloak، این پنجره را **باز نگه دارید**. در مرورگر همین سرور، آدرس زیر را با IP واقعی خودتان باز کنید؛ باید متن تنظیمات نمایش داده شود:

`https://SERVER-IP:8443/realms/kafdeck/.well-known/openid-configuration`

## ۷. ادمین اول Kafdeck را بسازید

یک **PowerShell جدید با دسترسی Administrator** و همان حساب ویندوز باز کنید:

<div dir="ltr">

```powershell
Set-Location C:\Kafdeck
.\scripts\Start-Kafdeck.ps1 -IssueSetupCode
.\scripts\Start-Kafdeck.ps1
```

</div>

دستور اول کد یک‌بارمصرف می‌دهد؛ آن را بردارید. کد **۱۵ دقیقه** اعتبار دارد. پنجرهٔ برنامه باز بماند.

آدرس `https://SERVER-IP:8080` را در مرورگر باز کنید. کد، ایمیل و رمز ادمین اول را بدهید؛ رمز حداقل ۱۲ کاراکتر باشد. **ایمیل تأیید ارسال نمی‌شود.** سپس از صفحهٔ Keycloak با همین حساب وارد شوید.

اگر کد منقضی شد، پیش از ساخت ادمین می‌توانید دستور IssueSetupCode را در پنجره‌ای دیگر تکرار کنید. پس از ساخته‌شدن ادمین، نصب را از اول مقداردهی نکنید و state را پاک نکنید.

## ۸. Kafka و کاربران را اضافه کنید

در داشبورد **Kafka connections** را بزنید، نامی مثل Arad Office بدهید و آدرس بروکرها را وارد کنید. برای اتصال فعلی Arad Office:

<div dir="ltr">

```text
192.168.100.33:19092,192.168.100.33:19093,192.168.100.33:19094
```

</div>

این IP و پورت‌ها باید از ویندوزسرور قابل دسترسی باشند. Security protocol را مطابق Kafka انتخاب کنید؛ اتصال فعلی Arad Office از **PLAINTEXT** استفاده می‌کند. **Test connection** سپس ذخیره/بازکردن را بزنید. کلاستر بالای داشبورد قابل انتخاب می‌شود.

برای کاربر دیگر **Manage users** را باز کنید. برای نمودارهای CPU/GC و نصب عامل، [راهنمای عامل Java 21](kafka-agent-java21.fa.md) را ادامه دهید. endpoint عامل باید آدرس **همین ویندوزسرور جدید** باشد.

برای ورود کاربران از کامپیوتر دیگر و ارسال عامل‌ها، پورت‌ها را فقط برای شبکهٔ داخلی مجاز باز کنید. در PowerShell سرور اجرا کنید؛ مثال شبکهٔ داخلی `192.168.100.0/24` است:

<div dir="ltr">

```powershell
$AllowedNetwork = Read-Host 'Allowed client and broker network (CIDR)'
New-NetFirewallRule -DisplayName 'Kafdeck HTTPS' -Direction Inbound -Action Allow -Protocol TCP -LocalPort 8080,8443 -RemoteAddress $AllowedNetwork
```

</div>

## ۹. مدیر دائمی سیستم ورود را بسازید

1. `https://SERVER-IP:8443/admin` را باز و با `bootstrap-admin` وارد شوید. realm همان **master** باشد.
2. **Users → Add user** را بزنید؛ کاربری مثل `identity-admin` با Enabled روشن بسازید.
3. در **Credentials → Set password** رمز قوی بدهید و **Temporary** را خاموش کنید.
4. در **Role mapping → Assign role**، **Filter by realm roles** و نقش **admin** را انتخاب و Assign کنید. این نقش را به کاربران عادی ندهید.
5. خارج شوید و ورود با `identity-admin` را امتحان کنید. **فقط پس از موفقیت**، کاربر موقت `bootstrap-admin` را حذف کنید.

ادمین Kafdeck در realm **kafdeck** است؛ حساب master برای نگهداری سیستم ورود است.

## ۱۰. اجرای خودکار پس از روشن‌شدن ویندوز

دو پنجرهٔ اجرای Keycloak و Kafdeck را با **Ctrl+C** متوقف کنید. در یک PowerShell با همان حساب نصب و دسترسی Administrator اجرا کنید:

<div dir="ltr">

```powershell
Set-Location C:\Kafdeck
.\scripts\Install-StartupTask.ps1 -Keycloak
.\scripts\Install-StartupTask.ps1
Start-ScheduledTask -TaskName Kafdeck-Keycloak
```

</div>

وقتی credential می‌خواهد، **نام کاربری و رمز همان حساب ویندوز نصب‌کننده** را بدهید؛ رمز ادمین Kafdeck یا postgres نیست. پس از آماده‌شدن آدرس Keycloak مرحلهٔ ۶، اجرا کنید:

<div dir="ltr">

```powershell
Start-ScheduledTask -TaskName Kafdeck
```

</div>

از این پس دو Task هنگام روشن‌شدن ویندوز اجرا می‌شوند؛ PostgreSQL هم سرویس ویندوز خودش را دارد. از Start، **Task Scheduler** را باز و نام‌های **Kafdeck** و **Kafdeck-Keycloak** را بررسی کنید. پنجرهٔ اجرای دستی را هم‌زمان روشن نگذارید.

## اگر مشکلی دیدید

| مشکل | راه‌حل |
| --- | --- |
| رمز PostgreSQL پذیرفته نشد | رمز مرحلهٔ ۳ را بزنید؛ رمز ادمین Kafdeck نیست |
| ساخت دیتابیس خطا داد | همان‌جا متوقف شوید؛ دیتابیس یا role موجود را پاک نکنید و متن خطا را برای پشتیبانی بفرستید |
| مرورگر هشدار گواهی داد | مرحلهٔ ۵ را روی همان کامپیوتری که مرورگر روی آن باز است انجام دهید؛ از IP ثبت‌شده استفاده کنید |
| پورت 8443 باز نمی‌شود | پنجرهٔ Keycloak و سرویس PostgreSQL را بررسی کنید |
| پورت 8080 باز نمی‌شود | پنجرهٔ Kafdeck و برای دسترسی از بیرون سرور، firewall مرحلهٔ ۸ را بررسی کنید |
| بعد از بوت روشن نمی‌شود | دو Task، حساب اجرای آن‌ها و سرویس PostgreSQL را بررسی کنید؛ برنامهٔ مربوط را دستی اجرا کنید تا خطا دیده شود |
| اتصال Kafka تست نمی‌شود | IP و پورت باید از همین سرور قابل دسترسی و میزبان در فهرست مرحلهٔ ۴ باشد |

**نگهداری:** از state، config و secrets، دیتابیس PostgreSQL و کلیدهای Data Protection حساب اجرا به‌صورت هماهنگ پشتیبان بگیرید. این‌ها را برای نصب مجدد پاک نکنید. جزئیات ارتقا و گواهی سازمانی در [راهنمای تکمیلی](windows-server-monitoring-advanced.fa.md) آمده است.

</div>
