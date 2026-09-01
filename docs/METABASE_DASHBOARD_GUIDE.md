# Hướng Dẫn Chi Tiết Tạo Dashboard Metabase — PitchFlow Gold

Tài liệu này hướng dẫn chi tiết từng bước xây dựng Dashboard **Premier League 2015/16 — Match & Team Analytics** trên Metabase từ bộ dữ liệu Gold của PitchFlow.

---

## 1. Dữ Liệu Đầu Vào (PITCHFLOW GOLD)

Hệ thống cung cấp 3 bảng dữ liệu đã qua làm sạch và tổng hợp:
1. `Gold Match Summary`: Thống kê từng trận đấu (bàn thắng, cú sút, thẻ phạt, đội thắng...).
2. `Gold Team Performance`: Thống kê tổng hợp theo từng đội bóng (trận đấu, thắng, hòa, thua, bàn thắng, bàn thua, điểm số).
3. `Gold Event Distribution`: Thống kê số lượng sự kiện (chuyền bóng, sút, phạm lỗi...) theo mốc thời gian (phút).

---

## 2. Hướng Dẫn Tạo Từng Biểu Đồ (Charts)

### 📌 Biểu đồ 1: Thẻ KPI Tổng số bàn thắng giải đấu (Card Number)
- **Mục đích**: Hiển thị 1 con số tổng duy nhất ấn tượng trên đầu Dashboard.
- **Các bước thực hiện**:
  1. Ở góc trên bên phải màn hình Metabase, nhấn nút **`+ New`** $\rightarrow$ chọn **`Question`** (hoặc nhấn **`Add a chart`** trên Dashboard).
  2. Chọn Data Source: **`PITCHFLOW GOLD`** $\rightarrow$ Chọn bảng **`Gold Match Summary`**.
  3. Ở thanh công cụ trên cùng bên phải, nhấn nút **`Summarize`**.
  4. Trong phần **Metrics**: chọn chỉ số `Sum of...` $\rightarrow$ chọn cột **`Goal Count`**.
  5. Ở góc dưới bên trái, nhấn nút **`Visualization`** $\rightarrow$ chọn icon **`Number`**.
  6. Nhấn nút **`Save`** (góc trên bên phải) $\rightarrow$ Đặt tên: `Tổng Số Bàn Thắng` $\rightarrow$ Chọn thêm vào Dashboard **`Premier League 2015/16 — Match & Team Analytics`**.

---

### 📌 Biểu đồ 2: Bảng xếp hạng điểm số các đội bóng (Leaderboard - Bar Chart)
- **Mục đích**: So sánh điểm số giữa các đội bóng.
- **Các bước thực hiện**:
  1. Nhấn **`+ New`** $\rightarrow$ **`Question`**.
  2. Chọn **`PITCHFLOW GOLD`** $\rightarrow$ Chọn bảng **`Gold Team Performance`**.
  3. Nhấn **`Summarize`**:
     - Metric: chọn `Sum of Points` (hoặc `Average of Points`).
     - Group by (ở bên dưới): chọn **`Team Name`**.
  4. Nhấn nút **`Visualization`** ở góc dưới bên trái $\rightarrow$ chọn icon **`Bar`** (Biểu đồ cột đứng).
  5. Để sắp xếp đội cao điểm nhất đứng đầu: Nhấn vào trục ngang hoặc nhấn icon **`Display`** (hình bánh răng) $\rightarrow$ Sort từ cao xuống thấp (Descending).
  6. Nhấn **`Save`** $\rightarrow$ Đặt tên: `Bảng Xếp Hạng Điểm Số` $\rightarrow$ Thêm vào Dashboard.

---

### 📌 Biểu đồ 3: Top các đội ghi bàn nhiều nhất (Top Scoring Teams - Row Chart)
- **Mục đích**: Hiển thị dạng thanh ngang dễ nhìn danh sách các đội ghi bàn hàng đầu.
- **Các bước thực hiện**:
  1. Nhấn **`+ New`** $\rightarrow$ **`Question`**.
  2. Chọn **`PITCHFLOW GOLD`** $\rightarrow$ Chọn bảng **`Gold Team Performance`**.
  3. Nhấn **`Summarize`**:
     - Metric: chọn `Sum of Goals For`.
     - Group by: chọn **`Team Name`**.
  4. Nhấn **`Visualization`** $\rightarrow$ chọn icon **`Row`** (Biểu đồ thanh ngang).
  5. Nhấn **`Save`** $\rightarrow$ Đặt tên: `Top Đội Ghi Bàn Nhiều Nhất` $\rightarrow$ Thêm vào Dashboard.

---

### 📌 Biểu đồ 4: Tỷ lệ Thắng - Hòa - Thua (Stacked Bar Chart)
- **Mục đích**: So sánh tương quan kết quả thi đấu của từng đội dưới dạng cột chồng.
- **Các bước thực hiện**:
  1. Nhấn **`+ New`** $\rightarrow$ **`Question`**.
  2. Chọn **`PITCHFLOW GOLD`** $\rightarrow$ Chọn bảng **`Gold Team Performance`**.
  3. Nhấn **`Summarize`**:
     - Metric: Thêm 3 chỉ số `Sum of Wins`, `Sum of Draws`, `Sum of Losses`.
     - Group by: chọn **`Team Name`**.
  4. Nhấn **`Visualization`** $\rightarrow$ chọn icon **`Bar`**.
  5. Nhấn icon **`Display`** (hình bánh răng ở góc dưới bên trái) $\rightarrow$ Mục **Stacking** chọn **`Stack`**.
  6. Nhấn **`Save`** $\rightarrow$ Đặt tên: `Tỷ Lệ Thắng - Hòa - Thua` $\rightarrow$ Thêm vào Dashboard.

---

### 📌 Biểu đồ 5: Diễn biến sự kiện trận đấu theo phút (Event Timeline - Line Chart)
- **Mục đích**: Theo dõi mật độ các loại sự kiện (chuyền bóng, sút, phạm lỗi...) diễn ra trong trận đấu theo từng mốc phút.
- **Các bước thực hiện**:
  1. Nhấn **`+ New`** $\rightarrow$ **`Question`**.
  2. Chọn **`PITCHFLOW GOLD`** $\rightarrow$ Chọn bảng **`Gold Event Distribution`**.
  3. Nhấn **`Summarize`**:
     - Metric: chọn `Sum of Event Count`.
     - Group by: chọn 2 trường là **`Minute Bucket`** và **`Event Type`**.
  4. Nhấn **`Visualization`** $\rightarrow$ chọn icon **`Line`** (Biểu đồ đường) hoặc **`Area`** (Biểu đồ vùng).
  5. Nhấn **`Save`** $\rightarrow$ Đặt tên: `Diễn Biến Sự Kiện Theo Mốc Phút` $\rightarrow$ Thêm vào Dashboard.

---

## 3. Sắp Xếp & Hoàn Thiện Dashboard

1. Mở Dashboard **`Premier League 2015/16 — Match & Team Analytics`**.
2. Kéo thả các thẻ biểu đồ vừa tạo để sắp xếp vị trí:
   - Đặt thẻ **KPI Number** (`Tổng Số Bàn Thắng`) ở trên cùng góc trái.
   - Đặt các biểu đồ cột `Bảng Xếp Hạng Điểm Số` và `Top Đội Ghi Bàn` ở giữa.
   - Đặt biểu đồ đường `Diễn Biến Sự Kiện Theo Mốc Phút` rộng ra ở phía dưới cùng.
3. Nhấn nút **`Save`** ở góc trên bên phải Dashboard để lưu giao diện hoàn chỉnh.

---

## 4. Giao Diện Mẫu & Mã Nhúng Live Embed (Dashboard Preview)

![Premier League 2015/16 Metabase Analytics Dashboard](images/dashboard_preview.png)

### Mã iframe nhúng Live Dashboard vào Web application:
Bật tính năng **Public Sharing** trong Metabase Dashboard settings và sử dụng đoạn mã iframe với public token đã tạo:

```html
<iframe
    src="http://localhost:3000/public/dashboard/62fcdade1dd1122d03f804dde9fae39fff070b0c5874e94977442344005dca5f"
    frameborder="0"
    width="100%"
    height="800"
    allowtransparency>
</iframe>
```

