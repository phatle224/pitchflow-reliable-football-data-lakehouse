# PitchFlow — Hướng dẫn chạy project

Tài liệu này là runbook thực hành cho pipeline Premier League 2015/16: chuẩn bị môi trường, khởi động Docker Compose, truyền input, chạy pipeline, kiểm tra output ở MinIO/PostgreSQL/Metabase, replay và xử lý lỗi thường gặp.

## 1. Tổng quan input → output

```text
Input:
  StatsBomb Open Data snapshot (commit SHA cố định)
  + DAG run config (match_limit, inject_chaos)
          |
          v
  ingest_raw
          |
          v
  Bronze Delta trên MinIO
          |
          v
  bronze_to_silver: parse + DQ + dedup
          |                         |
          v                         v
  Silver Delta                 Quarantine Delta
          |
          v
  silver_to_gold
          |
          v
  Gold Delta trên MinIO
          |
          v
  publish_serving
          |
          v
  PostgreSQL -> Metabase
```

Kết quả cuối không phải là một file CSV trong repository. Raw và Delta tables nằm trong Docker volume của MinIO; các bảng Gold phục vụ dashboard được UPSERT sang PostgreSQL.

## 2. Prerequisites

Cần cài:

- Docker Desktop đang chạy với Linux containers.
- Docker Compose v2 (`docker compose`, không phải `docker-compose` cũ).
- Git và PowerShell nếu muốn chạy validation trên host.
- Tối thiểu khoảng 6–8 GB RAM Docker cho Airflow, Spark, PostgreSQL, MinIO và Metabase.

Kiểm tra:

```powershell
docker version
docker compose version
python --version
```

Nếu `docker version` báo không kết nối được `dockerDesktopLinuxEngine`, hãy mở Docker Desktop trước.

## 3. Chuẩn bị cấu hình

Chạy từ repository root:

```powershell
Copy-Item .env.example .env
```

Các local defaults chính:

| Variable | Default | Dùng cho |
|---|---|---|
| `MINIO_ROOT_USER` | `minioadmin` | MinIO console/API |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | MinIO console/API |
| `PITCHFLOW_MINIO_BUCKET` | `pitchflow` | Bucket Delta |
| `PITCHFLOW_SOURCE_CONFIG` | `config/statsbomb_source.json` | Manifest source đang chạy (mặc định Premier League 2015/16) |
| `PITCHFLOW_MINIO_ENDPOINT` | `http://minio:9000` | Spark trong Docker |
| `POSTGRES_USER` | `pitchflow` | PostgreSQL |
| `POSTGRES_PASSWORD` | `pitchflow` | PostgreSQL |
| `PITCHFLOW_POSTGRES_DB` | `pitchflow` | Gold serving DB |
| `AIRFLOW_ADMIN_USERNAME` | `airflow` | Airflow UI |
| `AIRFLOW_ADMIN_PASSWORD` | `airflow` | Airflow UI |

Các password trên chỉ phù hợp local demo. Không commit `.env`; production phải dùng secret manager/IAM.

## 4. Khởi động stack

Build image và chạy background:

```powershell
docker compose up --build -d
```

Lần đầu có thể mất vài phút vì Docker build Airflow/Spark và Spark tải package Delta/Hadoop khi submit job.

Kiểm tra service:

```powershell
docker compose ps
docker compose logs --tail=100 airflow-init
docker compose logs --tail=100 minio-init
```

Expected: `postgres` healthy; Airflow webserver/scheduler, Spark master/worker, MinIO và Metabase ở trạng thái Up. `airflow-init` và `minio-init` có thể exited với code 0 vì đây là init jobs.

## 5. Các UI và credential local

| Service | URL | Credential/connection |
|---|---|---|
| Airflow | http://localhost:8088 | `airflow` / `airflow` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Spark Master UI | http://localhost:8080 | Không cần login |
| Metabase | http://localhost:3000 | Tạo admin account lần đầu |

Metabase kết nối PostgreSQL bằng hostname Docker `postgres` (không dùng `localhost`): database `pitchflow`, port `5432`, user/password `pitchflow`/`pitchflow`, SSL tắt, schema `public`. Ứng dụng chạy trực tiếp trên host (ví dụ DBeaver) dùng `localhost:5432` hoặc `127.0.0.1:5432`.

## 6. Airflow DAG và input

DAG là `pipeline_daily`, mặc định được tạo ở trạng thái paused để tránh tự tải snapshot khi stack vừa khởi động.

Unpause bằng UI hoặc command:

```powershell
docker compose exec -T airflow-scheduler airflow dags unpause pipeline_daily
```

Trigger DAG trong Airflow UI bằng **Trigger DAG → Config**. Config được hỗ trợ:

| Input | Type | Bắt buộc | Ý nghĩa |
|---|---|---:|---|
| `match_limit` | integer | Không | Giới hạn số match để smoke test; bỏ trống để lấy toàn bộ snapshot |
| `inject_chaos` | boolean | Không | Thêm synthetic duplicate/malformed/correction/late variants |

Smoke test khuyến nghị:

```json
{
  "match_limit": 2,
  "inject_chaos": true
}
```

Full run:

```json
{
  "inject_chaos": false
}
```

Airflow tự tạo `run_id`; DAG truyền giá trị đó thành `pipeline_run_id` cho cả bốn task.

## 7. Pipeline tasks và output trung gian

### `ingest_raw`

Input mặc định là `config/statsbomb_source.json`, gồm repository URL, raw base URL, commit SHA, competition `2` và season `27` (Premier League 2015/16, 380 matches). Có thể chọn profile World Cup cũ bằng `PITCHFLOW_SOURCE_CONFIG=config/statsbomb_world_cup_2022.json` hoặc option `--source-config`.

Output:

```text
s3a://pitchflow/bronze/source_records
```

Mỗi row giữ `raw_payload`, source locator, payload hash, commit SHA, ingestion timestamp và `pipeline_run_id`. Bronze dùng insert-only merge theo `bronze_record_id`.

### `bronze_to_silver`

Đọc Bronze của run hiện tại, parse matches/lineups/events và ghi:

```text
s3a://pitchflow/silver/matches
s3a://pitchflow/silver/teams
s3a://pitchflow/silver/stadiums
s3a://pitchflow/silver/players
s3a://pitchflow/silver/match_events
s3a://pitchflow/quarantine/match_events
s3a://pitchflow/ops/quality_metrics
```

Silver là typed/conformed data. Record lỗi đi Quarantine; không bị silently drop.

### `silver_to_gold`

Đọc Silver và rebuild match/team bị ảnh hưởng:

```text
s3a://pitchflow/gold/match_summary
s3a://pitchflow/gold/team_performance
s3a://pitchflow/gold/event_distribution
```

### `publish_serving`

Đọc Gold Delta và UPSERT vào PostgreSQL database `pitchflow`:

```text
gold_match_summary
gold_team_performance
gold_event_distribution
```

## 8. Output chi tiết

### Bronze

Bronze là raw source of truth và append-only. Dữ liệu vật lý là Parquet cùng thư mục `_delta_log` trong MinIO. Có thể xem bằng MinIO Console: chọn bucket `pitchflow`, mở prefix `bronze/source_records`.

### Silver

Silver chứa dữ liệu đã parse và typed:

- `matches`: thông tin match, score, kickoff và home/away teams.
- `teams`: team ID, name và country.
- `stadiums`: stadium ID, name và country.
- `players`: player ID, name, team và position nếu có.
- `match_events`: event ID, match/team/player, event type, minute, timestamp, `event_payload_hash`, `is_late`.

### Quarantine

`quarantine/match_events` lưu record không đạt DQ cùng `bronze_record_id`, `error_type`, `failed_rule`, `error_message`, `status`, `retry_count` và `rule_version`. Raw payload để replay vẫn được đọc từ Bronze.

### Quality metrics

`ops/quality_metrics` có một row cho mỗi `pipeline_run_id`:

- `input_event_rows`;
- `valid_event_rows`;
- `quarantine_event_rows`;
- `duplicate_event_rows`;
- `late_event_count`;
- `dq_pass_rate`;
- `measured_at`.

### Gold

- `match_summary`: một row/match, score, winner, goals, shots, cards và total events.
- `team_performance`: một row/team, matches played, wins/draws/losses, goals và points.
- `event_distribution`: event type theo 15-minute bucket.

## 9. Kiểm tra output bằng PostgreSQL

Xem row counts:

```powershell
docker compose exec -T postgres psql -U pitchflow -d pitchflow -c "SELECT 'match_summary' AS table_name, count(*) FROM gold_match_summary UNION ALL SELECT 'team_performance', count(*) FROM gold_team_performance UNION ALL SELECT 'event_distribution', count(*) FROM gold_event_distribution;"
```

Xem sample match summary:

```powershell
docker compose exec -T postgres psql -U pitchflow -d pitchflow -c "SELECT match_id, home_team_name, away_team_name, home_score, away_score, winner, event_count FROM gold_match_summary ORDER BY match_id LIMIT 10;"
```

Xem team performance:

```powershell
docker compose exec -T postgres psql -U pitchflow -d pitchflow -c "SELECT team_name, matches_played, wins, draws, losses, points, goal_difference FROM gold_team_performance ORDER BY points DESC, goal_difference DESC;"
```

Xem DQ metrics trong Spark container:

```powershell
docker compose exec -T airflow-scheduler spark-submit --master spark://spark-master:7077 --deploy-mode client --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.executorEnv.PYTHONPATH=/opt/pitchflow /opt/pitchflow/spark/jobs/bronze_to_silver.py --pipeline-run-id <run-id>
```

Lệnh trên là transformation command, không nên chạy lại chỉ để đọc metrics trong môi trường có dữ liệu production. Dùng MinIO/Spark SQL hoặc một query utility riêng khi cần inspect metrics mà không mutate.

## 10. Kiểm tra output bằng MinIO

Mở `http://localhost:9001`, đăng nhập `minioadmin`/`minioadmin`, chọn bucket `pitchflow` và kiểm tra các prefix:

```text
bronze/source_records/
silver/matches/
silver/match_events/
quarantine/match_events/
gold/match_summary/
gold/team_performance/
gold/event_distribution/
ops/quality_metrics/
```

Mỗi Delta table phải có Parquet data và `_delta_log`. `_delta_log` là bằng chứng table được quản lý bởi Delta, không chỉ là file Parquet rời rạc.

## 11. Xem kết quả trong Metabase

1. Mở `http://localhost:3000` và tạo admin account lần đầu.
2. Chọn **Add a database → PostgreSQL**.
3. Điền host `postgres`, port `5432`, database `pitchflow`, user `pitchflow`, password `pitchflow`.
4. Tắt SSL và chọn schema `public`.
5. Sync database metadata.
6. Tạo question từ `gold_match_summary`, `gold_team_performance` hoặc `gold_event_distribution`.
7. Gộp các question thành dashboard football analytics.

Metabase không đọc trực tiếp MinIO trong V1; dashboard luôn query PostgreSQL serving tables.

## 12. Rerun và kiểm tra idempotency

Trigger lại cùng logical input hoặc rerun failed task. Kỳ vọng:

- Bronze không thêm record có cùng `bronze_record_id`.
- Silver events không thêm row có cùng `event_id`.
- Gold không tăng duplicate business rows.
- PostgreSQL UPSERT giữ nguyên số row và cập nhật giá trị mới nếu có.
- `quality_metrics` merge theo `pipeline_run_id`.

Để chứng minh bằng số liệu, lưu row counts trước rerun, rerun DAG, rồi chạy lại các câu lệnh ở mục 9. Đây là test quan trọng hơn việc chỉ kiểm tra task có màu xanh.

V2 cung cấp hai DAG riêng để vận hành Replay và Correction mà không cần chạy lệnh thủ công:

1. **DAG `pipeline_replay`**: Replay một hoặc nhiều `bronze_record_id` qua Silver $\rightarrow$ Gold $\rightarrow$ Serving.
   - Trigger config:
     ```json
     {
       "bronze_record_ids": ["<bronze-record-id>"],
       "match_ids": ["<match-id>"],
       "replay_reason": "Re-processing fixed raw payload"
     }
     ```
2. **DAG `pipeline_resolve_correction`**: Xử lý phê duyệt/từ chối record bị Quarantine do thay đổi payload.
   - Trigger config phê duyệt:
     ```json
     {
       "quarantine_ids": ["<quarantine-id>"],
       "action": "approve",
       "resolution_note": "Verified correction",
       "match_ids": ["<match-id>"]
     }
     ```

Replay không sửa raw Bronze; record tự động cập nhật trạng thái `REPROCESSED` trong Quarantine và tăng `retry_count` kèm audit log.

## 14. Chạy controlled chaos

Để kiểm tra reliability, trigger DAG với:

```json
{
  "match_limit": 2,
  "inject_chaos": true
}
```

Generator tạo từ event hợp lệ các trường hợp exact duplicate, missing event ID, invalid minute, changed-payload correction và late event. Kết quả kỳ vọng là record hợp lệ đi Silver/Gold; record invalid/correction đi Quarantine; duplicate được đo nhưng không nhân đôi.

Một smoke run đã xử lý 3.393 event rows, giữ 3.389 valid, deduplicate 1 và quarantine 3 deliberate variants. Con số có thể khác nếu source/config hoặc giới hạn match thay đổi; hãy dùng metrics của chính `run_id` để đánh giá.

## 15. Theo dõi Airflow

Trong Airflow UI:

1. Mở DAG `pipeline_daily`.
2. Xem graph để biết task nào fail.
3. Mở task log, đặc biệt `ingest_raw` nếu source download lỗi và `bronze_to_silver` nếu DQ/reference lỗi.
4. Copy `run_id` từ DAG run để truy vết qua Bronze, quality metrics và serving `pipeline_run_id`.
5. Rerun task sau khi xác định nguyên nhân; kiểm tra idempotency sau rerun.

DAG hiện có bốn task chính nhưng V2 vẫn cần bổ sung retry/backoff và failure notifications production-grade.

## 16. Troubleshooting

### Docker daemon không chạy

Triệu chứng: `failed to connect to the docker API ... dockerDesktopLinuxEngine`.

Khắc phục: mở Docker Desktop, chờ engine healthy, rồi chạy `docker compose ps` và `docker compose up -d`.

### Container init exited

`airflow-init` và `minio-init` exited code 0 là bình thường. Nếu exited code khác 0, xem:

```powershell
docker compose logs airflow-init
docker compose logs minio-init
```

### Airflow chưa hiện DAG

Kiểm tra scheduler đã Up và file được mount ở `/opt/pitchflow/airflow/dags`. Xem scheduler logs:

```powershell
docker compose logs --tail=200 airflow-scheduler
```

### DAG bị paused

Đây là default có chủ ý. Unpause:

```powershell
docker compose exec -T airflow-scheduler airflow dags unpause pipeline_daily
```

### Metabase không kết nối PostgreSQL

Trong form Metabase dùng host `postgres`, không dùng `localhost`. Xác nhận `postgres` healthy và database là `pitchflow`; ứng dụng chạy ngoài Docker (DBeaver/psql) dùng `localhost:5432` hoặc `127.0.0.1:5432`.

### Spark không đọc được MinIO

Kiểm tra `PITCHFLOW_MINIO_ENDPOINT=http://minio:9000`, bucket `pitchflow`, credentials và trạng thái `minio-init`. Trong Docker phải dùng service name `minio`, không dùng `localhost:9000`.

### `bronze_to_silver` báo thiếu Silver matches

Events cần match reference để DQ. Chạy ingestion đầy đủ hoặc bảo đảm batch có object `matches` trước khi xử lý events; không bỏ qua bước `ingest_raw` khi khởi tạo dữ liệu mới.

### Rerun làm số liệu bất thường

Không xóa volume ngay lập tức. Ghi lại `run_id`, xem task logs, kiểm tra Bronze/Silver/Gold counts và quality metrics. Chỉ reset dữ liệu local khi chấp nhận mất toàn bộ state; đây là thao tác destructive.

## 17. Dừng và reset local stack

Dừng container nhưng giữ dữ liệu:

```powershell
docker compose stop
```

Dừng và remove container/network nhưng giữ named volumes:

```powershell
docker compose down
```

Reset toàn bộ local data (MinIO, PostgreSQL, Airflow logs) — chỉ dùng khi muốn bắt đầu lại từ đầu:

```powershell
docker compose down -v
```

Lệnh cuối xóa state local không thể phục hồi từ Docker volume; hãy xác nhận trước khi chạy.

## 18. Validation trước khi bàn giao

Chạy từ repository root:

```powershell
python -m unittest discover -s tests -v
python scripts/validate_project.py
docker compose config --quiet
git diff --check
```

Unit/harness checks hiện chạy không cần cài dependency ngoài. Docker-backed smoke test cần Docker Desktop đang chạy và được mô tả ở mục 6, 14.

## 19. Tham chiếu source of truth

- `docker-compose.yml`: topology, ports, volumes và environment defaults.
- `config/statsbomb_source.json`: source URI, commit SHA và competition/season.
- `airflow/dags/pipeline_daily.py`: DAG và thứ tự task.
- `spark/jobs/ingest_raw.py`: input ingestion.
- `spark/jobs/bronze_to_silver.py`: Silver, DQ, Quarantine và metrics.
- `spark/jobs/silver_to_gold.py`: Gold aggregates.
- `serving/publish_postgres/publish.py`: PostgreSQL serving UPSERT.
- `docs/testing.md`: expected smoke-test behavior.
