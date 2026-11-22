namespace Kafdeck.Setup;

internal sealed class Language
{
    public bool Persian { get; set; }
    public string this[string key] => Words.TryGetValue(key, out var value) ? (Persian ? value.Fa : value.En) : throw new ArgumentException(key);
    private static readonly Dictionary<string, (string En, string Fa)> Words = new()
    {
        ["title"] = ("Kafdeck Offline Setup", "نصب آفلاین Kafdeck"),
        ["welcome"] = ("Welcome to Kafdeck", "به Kafdeck خوش آمدید"),
        ["intro"] = ("Install or upgrade Kafdeck, Java, PostgreSQL and Keycloak using this single offline file. Internet access is not required on the server. Windows Server 2019 or newer is required.", "نصب یا ارتقای Kafdeck به همراه Java، PostgreSQL و Keycloak با همین یک فایل آفلاین انجام می‌شود. سرور به اینترنت نیاز ندارد. ویندوز سرور ۲۰۱۹ یا جدیدتر لازم است."),
        ["accountNotice"] = ("Run as the Windows account that will own the installation. Services use that account; upgrades require the original account to preserve encrypted secrets.", "نصب را با حساب ویندوزی مالک نصب اجرا کنید. سرویس‌ها با همان حساب اجرا می‌شوند؛ برای حفظ اطلاعات رمز‌شده، ارتقا نیز باید با حساب اولیه انجام شود."),
        ["language"] = ("Setup language", "زبان نصب"),
        ["next"] = ("Next", "بعدی"), ["back"] = ("Back", "قبلی"), ["close"] = ("Close", "بستن"),
        ["agreement"] = ("License agreement", "توافقنامه و مجوز"),
        ["terms"] = ("Kafdeck is distributed by Arad ITC under Apache License 2.0, without warranty. Included third-party components retain their own licenses. Acceptance does not replace or restrict those licenses. Review both tabs before continuing.", "Kafdeck توسط Arad ITC با مجوز Apache 2.0 و بدون ضمانت عرضه می‌شود. اجزای جانبی مجوز مستقل خود را دارند. پذیرش این متن جایگزین یا محدودکنندهٔ آن مجوزها نیست. پیش از ادامه، هر دو زبانه را مطالعه کنید. متن انگلیسی مجوز، متن اصلی است."),
        ["accept"] = ("I accept the Apache 2.0 license and acknowledge the included third-party terms.", "مجوز Apache 2.0 را می‌پذیرم و شرایط مجوز اجزای جانبی را مطالعه کرده‌ام."),
        ["notices"] = ("Third-party notices", "مجوز اجزای جانبی"),
        ["configuration"] = ("Installation settings", "تنظیمات نصب"),
        ["action"] = ("Operation", "عملیات"), ["install"] = ("Install / resume / restore retained services", "نصب / ادامهٔ نصب / بازیابی سرویس‌های نگه‌داشته‌شده"),
        ["upgrade"] = ("Upgrade existing installation", "ارتقای نصب موجود"), ["remove"] = ("Remove services (retain files and all data)", "حذف سرویس‌ها (حفظ تمام فایل‌ها و داده‌ها)"),
        ["root"] = ("Application directory (ASCII, no spaces)", "پوشهٔ برنامه (حروف انگلیسی، بدون فاصله)"),
        ["address"] = ("This server's private IPv4 address", "آدرس IPv4 داخلی همین سرور"),
        ["kafka"] = ("Kafka host IPv4 addresses (comma separated, no ports)", "آدرس IPv4 سرورهای Kafka (با ویرگول، بدون پورت)"),
        ["clients"] = ("Allowed client networks (LocalSubnet, IPv4 or CIDR, comma separated)", "شبکه‌های مجاز کاربران (LocalSubnet، IPv4 یا CIDR، با ویرگول)"),
        ["appPort"] = ("Kafdeck HTTPS port", "پورت HTTPS کافدک"), ["identityPort"] = ("Keycloak HTTPS port", "پورت HTTPS کی‌کلاک"),
        ["databasePort"] = ("Local PostgreSQL port", "پورت محلی PostgreSQL"),
        ["account"] = ("Windows service account", "حساب ویندوز سرویس‌ها"),
        ["password"] = ("Current Windows account password (not a PIN)", "رمز حساب فعلی ویندوز (نه PIN)"),
        ["passwordHelp"] = ("Required for a new or resumed installation only. Passed in memory to Windows SCM, never saved in setup files or command arguments.", "فقط برای نصب یا ادامهٔ نصب لازم است. رمز در حافظه به مدیریت سرویس‌های ویندوز داده می‌شود و در فایل‌های نصب یا آرگومان‌های فرمان ذخیره نمی‌شود."),
        ["review"] = ("Ready to proceed", "آمادهٔ اجرا"), ["start"] = ("Start", "شروع"),
        ["reviewText"] = ("Services start automatically after reboot. Upgrade creates a verified cold backup and retains users, cluster settings, databases and encryption keys. Removal stops and unregisters only owned services; all files, databases and recovery material remain. Never delete the retained installation directory without a verified backup.", "سرویس‌ها پس از راه‌اندازی مجدد ویندوز خودکار اجرا می‌شوند. ارتقا نسخهٔ پشتیبان بررسی‌شده می‌سازد و کاربران، تنظیمات کلاسترها، دیتابیس‌ها و کلیدهای رمزنگاری را حفظ می‌کند. حذف، فقط سرویس‌های همین نصب را متوقف و لغو ثبت می‌کند؛ فایل‌ها، دیتابیس‌ها و اطلاعات بازیابی باقی می‌مانند. پوشهٔ نصب را بدون پشتیبان معتبر پاک نکنید."),
        ["working"] = ("Setup is running. Do not restart Windows or terminate this process. Initial Keycloak startup can take several minutes.", "نصب در حال اجراست. ویندوز را ری‌استارت نکنید و برنامه را نبندید. اولین اجرای Keycloak ممکن است چند دقیقه طول بکشد."),
        ["verifying"] = ("Verifying and preparing embedded offline files…", "بررسی و آماده‌سازی فایل‌های آفلاین داخل بسته…"),
        ["verified"] = ("Offline files verified.", "صحت فایل‌های آفلاین تأیید شد."),
        ["running"] = ("Installing, upgrading or removing owned Windows services…", "در حال نصب، ارتقا یا حذف سرویس‌های متعلق به همین نصب…"),
        ["complete"] = ("Setup completed successfully", "عملیات با موفقیت انجام شد"),
        ["firstAdmin"] = ("Create the first administrator at the address below. The one-time code expires 15 minutes after issuance. No email verification is needed. Keep this code private.", "ادمین اول را از آدرس زیر بسازید. کد یک‌بارمصرف ۱۵ دقیقه پس از صدور منقضی می‌شود. تأیید ایمیل لازم نیست. این کد محرمانه است."),
        ["login"] = ("The administrator already exists. Sign in at the address below.", "ادمین قبلاً ساخته شده است. از آدرس زیر وارد شوید."),
        ["code"] = ("One-time setup code", "کد یک‌بارمصرف راه‌اندازی"), ["open"] = ("Open Kafdeck", "باز کردن Kafdeck"),
        ["reboot"] = ("Windows restart required", "راه‌اندازی مجدد ویندوز لازم است"),
        ["rebootHelp"] = ("A prerequisite requires a Windows restart. Restart when convenient, then run this same EXE again with the same Windows account and installation directory. Existing data has been retained. Setup does not restart Windows automatically.", "یکی از پیش‌نیازها به راه‌اندازی مجدد ویندوز نیاز دارد. پس از ری‌استارت، همین EXE را با همان حساب و پوشهٔ نصب دوباره اجرا کنید. داده‌ها حفظ شده‌اند. نصب‌کننده ویندوز را خودکار ری‌استارت نمی‌کند."),
        ["removed"] = ("Owned services removed; files and all data retained", "سرویس‌های همین نصب حذف شدند؛ همهٔ فایل‌ها و داده‌ها حفظ شدند"),
        ["recovered"] = ("Previous installation recovered", "نصب قبلی بازیابی شد"),
        ["recoveredHelp"] = ("An interrupted upgrade was recovered. The new package has not been installed. Run this same EXE again and choose Upgrade to apply it. Existing registration and data have been retained.", "ارتقای نیمه‌تمام بازیابی شد و بستهٔ جدید هنوز نصب نشده است. همین EXE را دوباره اجرا و ارتقا را انتخاب کنید. ثبت برنامه و داده‌های قبلی حفظ شده‌اند."),
        ["removedHelp"] = ("To restore service registration, run this exact EXE again, select Install / resume, use the same directory and Windows account, and enter its password. A newer package must first use the supported upgrade path on a running installation.", "برای ثبت دوبارهٔ سرویس‌ها همین EXE را اجرا کنید، نصب / ادامه را انتخاب کنید و همان پوشه، حساب ویندوز و رمز آن را بدهید. برای بستهٔ جدیدتر، ابتدا نصب قبلی را بازیابی و سپس مسیر ارتقا را اجرا کنید."),
        ["failed"] = ("Setup could not finish", "عملیات کامل نشد"),
        ["failedHelp"] = ("Do not delete or reset the installation. Check the account, installation directory, port availability and logs under the installation's logs directory. An interrupted installation can be resumed with the same EXE; a failed upgrade retains its recovery journal and backup. Contact info@arad-itc.org if recovery is required.", "پوشهٔ نصب را پاک یا بازنشانی نکنید. حساب ویندوز، مسیر نصب، آزاد بودن پورت‌ها و لاگ‌های پوشهٔ logs را بررسی کنید. نصب متوقف‌شده با همین EXE قابل ادامه است؛ ارتقای ناموفق دفتر ثبت بازیابی و پشتیبان خود را حفظ می‌کند. برای بازیابی با info@arad-itc.org تماس بگیرید."),
        ["invalid"] = ("Check the settings: enter valid IPv4 addresses, approved client networks, three different ports, an ASCII installation path without spaces, and the Windows password for installation.", "تنظیمات را بررسی کنید: آدرس‌های معتبر IPv4، شبکه‌های مجاز، سه پورت متفاوت، مسیر انگلیسی بدون فاصله و رمز حساب ویندوز برای نصب لازم است."),
        ["missing"] = ("This development build has no verified offline payload. Build with build-windows-setup.py before distribution.", "این خروجی توسعه بستهٔ آفلاین معتبر ندارد. برای توزیع، از build-windows-setup.py استفاده کنید."),
        ["about"] = ("Arad ITC • https://arad-itc.com • info@arad-itc.org", "Arad ITC • https://arad-itc.com • info@arad-itc.org")
    };
    internal static IEnumerable<string> Keys => Words.Keys;
}
