# Water Network AI Monitoring — Streaming Data Pipeline

A Python TCP client that connects to a simulated smart-water-network AI monitoring
server (`water_ai_stream_server.py`), consumes a live JSON record stream, validates
each reading, splits clean vs. bad records, and emits a rolling summary report.

This repo was built for the **Hamrah Aval Academy — Data Engineer Bootcamp 2**
entrance challenge.

## What it does

1. **Ingests a live stream** — opens a TCP connection to `localhost:9034`, where the
   provided server pushes one newline-delimited JSON record roughly every 25 ms
   (~40 records/sec under normal conditions).
2. **Handles TCP framing** — since TCP is a byte stream with no built-in message
   boundaries, incoming bytes are buffered and split on `\n` before being parsed
   as JSON.
3. **Validates every record** against the schema below and collects *all* errors
   for a record (a record can fail more than one rule at once).
4. **Splits output**:
   - `clean_readings.csv` — records that pass every validation rule.
   - `bad_readings.csv` — records that fail one or more rules (plus the list of
     errors, or `invalid_json` if the line couldn't even be parsed).
5. **Reports every 20 seconds** to `real_time_reports.csv`: total records seen,
   clean count, bad count, and number of distinct active stations in that window.

## Data schema

| Field | Rule |
|---|---|
| `reading_id` | positive integer |
| `station_id` | one of `ST-01` … `ST-10` |
| `model_name` | `leak_detector`, `pressure_drop_predictor`, or `demand_forecaster` |
| `predicted_label` | depends on `model_name` — `leak`/`normal`, `drop`/`stable`, or an integer `1–5000` |
| `confidence_score` | float in `[0, 1]` |
| `response_time_ms` | positive number |
| `timestamp` | a valid ISO-8601 timestamp |

## Repo contents

| File | Purpose |
|---|---|
| `water_ai_stream_server.py` | Provided simulator — generates the record stream, including a randomized corruption rate and (optional) model drift over time. Not part of the deliverable, just the data source. |
| `solution.ipynb` | The client: TCP framing (`stream_records`), validation (`validate`), and the ingest/report loop (`main`). |
| `clean_readings.csv` / `bad_readings.csv` / `real_time_reports.csv` | Output produced by a run (sample output included). |
| `README.md` | This file. |

## How to run

```bash
# Terminal 1 — start the simulated stream
python3 water_ai_stream_server.py

# Terminal 2 — run the consumer (open solution.ipynb and run all cells,
# or extract main() into a .py script and run it)
jupyter notebook solution.ipynb
```

Stop the server with `Ctrl+C`; stop the consumer notebook cell the same way (it
closes the socket in a `finally` block).

## Known limitations of the current implementation

Worth being upfront about, since the challenge questions below ask about exactly
these scaling/reliability gaps:

- **Single connection only.** `main()` calls `stream_records()` once — it does not
  accept multiple simultaneous producers.
- **Per-row, unbuffered CSV writes**, with an explicit `flush()` on every single
  bad record — this is a lot of syscalls per second and won't hold up at high
  throughput.
- **No durability**: if the process crashes between reading a record off the
  socket and writing it to disk, that record is gone. There's no queue, WAL, or
  acknowledgment step.
- **No reconnect logic**: if the TCP connection drops, `stream_records()` simply
  ends (its `finally` closes the socket) and the generator loop exits — production
  code would retry with backoff.
- **`buffer += ...`** does repeated string concatenation, which is O(n) per
  append; fine at ~40 records/sec, not fine at 50,000/sec (see Q1 below).

---

## پاسخ به سؤالات چالش

### ۱. افزایش حجم داده (۵۰ → ۵۰,۰۰۰ رکورد در ثانیه)

**خیر، ترکیب فعلی «پایتون تک‌فرایندی + نوشتن مستقیم روی CSV» پاسخگو نیست.**
گلوگاه‌های اصلی پیاده‌سازی فعلی:

- **پردازش تک‌رشته‌ای و GIL**: حلقه‌ی `recv → split → json.loads → validate → writerow`
  کاملاً سریالی و روی یک هسته اجرا می‌شود. در ۴۰ رکورد/ثانیه مشکلی نیست؛ در
  ۵۰,۰۰۰ رکورد/ثانیه، پارس JSON و اعتبارسنجی به‌تنهایی از ظرفیت یک هسته
  عبور می‌کنند.
- **ساخت بافر با رشته (`buffer += ...`)**: الحاق رشته در پایتون تقریباً O(n) است؛
  در نرخ بالا این عملیات خودش تبدیل به گلوگاه CPU می‌شود (باید از `bytearray`/
  `bytes` و `split(b'\n')` استفاده کرد).
- **نوشتن سطر‌به‌سطر روی CSV با `flush()` مکرر**: هر `flush()` یک I/O سنکرون به
  دیسک است. نوشتن ۵۰,۰۰۰ سطر در ثانیه با فلاش مکرر عملاً غیرممکن است؛
  حتی بدون فلاش، فایل تخت (CSV) قابلیت نوشتن هم‌زمان چند نویسنده، فشرده‌سازی،
  ایندکس یا کوئری تحلیلی ندارد.
- **یک اتصال = یک تولیدکننده**: معماری فعلی برای چند منبع/مدل هم‌زمان طراحی
  نشده (به سؤال ۲ نگاه کنید).

**پیشنهاد معماری مقیاس‌پذیرتر:**

1. **جداسازی دریافت از پردازش با یک صف پیام دوام‌دار** (Kafka، Redis Streams یا
   RabbitMQ). سرویس دریافت فقط از سوکت می‌خواند و بلافاصله پیام خام را در صف
   می‌نویسد (بدون اعتبارسنجی سنگین)؛ این هم دریافت را از پردازش جدا می‌کند و هم
   یک لایه‌ی دوام (durability) قبل از هر پردازشی ایجاد می‌کند.
2. **موازی‌سازی افقی**: چند مصرف‌کننده (consumer) مستقل از پارتیشن‌های صف
   بخوانند و اعتبارسنجی/پردازش را موازی انجام دهند؛ به جای چند رشته در یک
   پروسه (که با GIL محدود می‌شوند)، از چند پروسه یا چند instance مستقل استفاده شود.
3. **نوشتن دسته‌ای (batching)** به‌جای هر رکورد یک بار: رکوردها را در حافظه
   بافر کرده و هر N رکورد یا هر چند صد میلی‌ثانیه یک‌جا بنویسیم.
4. **جایگزینی CSV با ذخیره‌سازی ستونی/پارتیشن‌بندی‌شده**: مثلاً فایل‌های Parquet
   پارتیشن‌شده بر اساس ایستگاه/ساعت، یا نوشتن مستقیم در یک دیتابیس
   سری‌زمانی مناسب برای تله‌متری با نرخ بالا (ClickHouse، TimescaleDB، InfluxDB).
5. **گزارش لحظه‌ای با پردازش جریانی واقعی**: به‌جای شمارنده‌های دستی در حلقه،
   از یک stream processor (مثل Kafka Streams/Flink یا حتی aggregation‌های
   windowed در سمت دیتابیس سری‌زمانی) برای محاسبه‌ی معیارهای هر بازه استفاده شود.

### ۲. چند جریان هم‌زمان (هر مدل، یک اتصال سوکت مستقل)

پیاده‌سازی فعلی فقط یک اتصال را می‌پذیرد. برای چند اتصال هم‌زمان دو رویکرد
معقول است:

- **مدل رشته‌ای (threading)**: به ازای هر اتصال ورودی یک ترد جداگانه اجرا شود
  که فقط کار `recv` + بافر + پارس JSON آن اتصال را انجام می‌دهد. اما نوشتن روی
  فایل‌های CSV و به‌روزرسانی شمارنده‌های مشترک (`total`, `clean`, `bad`,
  `active_stations`) باید **سریالی** بماند تا دو ترد هم‌زمان یک خط را قاطی
  ننویسند یا یک شمارنده را race condition نکنند؛ ساده‌ترین راه این است که هر ترد
  رکورد اعتبارسنجی‌شده را در یک `queue.Queue` مشترک بگذارد و **یک** ترد نویسنده
  (single-writer) از صف بخواند و روی دیسک بنویسد. قفل (`threading.Lock`) هم برای
  ساختارهای مشترک (مثل `set` ایستگاه‌های فعال) لازم است.
- **مدل asyncio**: به‌جای ترد به ازای هر اتصال، یک coroutine به ازای هر اتصال
  با `asyncio.start_server`؛ چون کار عمدتاً I/O-bound است (انتظار برای دیتای
  سوکت)، asyncio سربار کمتری نسبت به تردهای متعدد دارد و مقیاس بهتری در تعداد
  زیاد اتصالات هم‌زمان می‌دهد. باز هم نوشتن نهایی روی فایل باید از طریق یک
  task/queue واحد انجام شود.
- **گزارش‌دهی مستقل از هر اتصال**: تایمر ۲۰ ثانیه‌ی گزارش نباید به چرخه‌ی
  یک اتصال خاص گره بخورد (در کد فعلی، گزارش داخل حلقه‌ی یک generator است)؛
  باید یک تایمر/ترد جداگانه هر ۲۰ ثانیه، مقادیر شمارنده‌های مشترک را (زیر قفل)
  بخواند، در گزارش بنویسد و ریست کند.

### ۳. جلوگیری از هدر رفت داده (قطع اتصال / کرش / خطای ذخیره‌سازی)

چون رکوردهای گم‌شده می‌توانند نشانه‌ی یک نشتی واقعی باشند، معماری باید طوری
باشد که «دریافت شدن» و «ماندگار شدن» یک رکورد از هم جدا نشوند مگر با تأیید:

- **صف پیام دوام‌دار به‌عنوان بافر ورودی**: بلافاصله پس از پارس هر خط JSON،
  آن را (پیش از اعتبارسنجی سنگین) در یک صف دوام‌دار مثل Kafka یا Redis Streams
  بنویسیم. اگر پردازشگر پایین‌دستی کرش کند، پیام هنوز در صف است و بعد از
  ری‌استارت دوباره قابل خواندن است.
- **الگوی at-least-once + تأیید دریافت (ack) بعد از نوشتن موفق**: offset/پیام
  فقط زمانی commit یا ack شود که رکورد با موفقیت در ذخیره‌سازی نهایی نوشته
  شده باشد، نه زودتر. یعنی ترتیب درست: `دریافت → صف → پردازش → نوشتن دائمی
  → ack`.
- **Write-Ahead Log محلی** به‌عنوان لایه‌ی دفاعی اضافه: پیش از هر پردازش،
  خط خام را در یک فایل append-only با `fsync` بنویسیم؛ در صورت کرش پردازش،
  با ری‌استارت از روی WAL دوباره پردازش را از نقطه‌ی متوقف‌شده ادامه می‌دهیم.
- **Idempotency با `reading_id`**: چون at-least-once ممکن است باعث تحویل
  مجدد یک رکورد شود، نوشتن نهایی باید بر اساس `reading_id` idempotent باشد
  (مثلاً upsert به‌جای append خام، یا حذف تکراری‌ها در لایه‌ی گزارش) تا رکورد
  دوباره دو بار شمارش نشود.
- **مدیریت قطع اتصال TCP**: کلاینت باید با backoff نمایی به‌طور خودکار
  reconnect کند؛ چون منبع فعلی (سرور شبیه‌ساز) قابلیت replay ندارد، در یک
  سیستم واقعی، تولیدکننده‌ی داده (مدل‌ها) باید خودش هنگام قطع ارتباط مصرف‌کننده،
  داده را در یک صف محلی بافر کند تا از دست نرود.
- **مدیریت خطای ذخیره‌سازی**: نوشتن روی دیسک/دیتابیس در `try/except` قرار
  گیرد؛ در صورت خطا (پر شدن دیسک، خطای مجوز و…) رکورد به یک صف
  «dead-letter» یا retry-queue منتقل شود و لاگ گردد، نه اینکه بی‌صدا از دست برود.
- **جایگزینی append خام روی CSV با یک مخزن تراکنشی**: append روی فایل CSV هیچ
  تضمین atomicity ندارد (نوشتن نیمه‌کاره در اثر کرش می‌تواند فایل را خراب کند).
  یک دیتابیس با تراکنش (حتی SQLite با WAL mode) یا دیتابیس سری‌زمانی مناسب،
  تضمین بهتری برای صحت داده در برابر کرش می‌دهد.
