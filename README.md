# Đã sửa so với bản gốc

1. **Futures API thay Spot**: `fapi.binance.com` (trước là `api.binance.com`).
2. **Danh sách symbol** (`loadSymbolList()`, chạy ĐÚNG 1 LẦN lúc start): giao
   giữa `exchangeInfo` (USDT + PERPETUAL + TRADING) và `ticker/24hr` (phải
   có mặt ở cả 2) - khớp CHÍNH XÁC `_allSymbols` bên app Flutter
   (`explore_screen.dart`). Không lọc thêm `excludeKeywords` (USDC/BUSD/
   UP/DOWN...) vì Explore không lọc phần đó.
3. **1 request/symbol/chu kỳ thay vì 6**: lấy `limit=250` nến 1 phút 1 lần,
   suy ra CẢ 6 khung (1m/5m/15m/30m/1h/4h) bằng cách đọc đúng vị trí trong
   mảng đã có sẵn (`priceAtMinutesAgo()`) - giảm ~3 lần số request, tránh
   vượt trần 2400 weight/phút của Binance (xem tính toán trong comment đầu
   file `index.js`).
4. **Chủ động refresh nền** (`setInterval` mỗi 90s), KHÔNG còn kiểu "lazy"
   chỉ fetch khi có request tới và cache hết hạn - request luôn đọc cache
   có sẵn, gần như 0ms.
5. Thêm `/health` - xem server đã tải xong symbol list + cache lần đầu
   chưa, chu kỳ refresh gần nhất mất bao lâu.
6. Validate `window` theo whitelist rõ ràng (400 nếu sai) thay vì âm thầm
   trả rỗng.

# Cách test nhanh sau khi deploy

```
curl https://<url-render-của-bạn>/health
curl https://<url-render-của-bạn>/api/history_prices?window=4h
```

`/health` nên thấy `symbolCount` > 0 sau vài giây, và `cachedSymbolCount`
tăng dần lên gần bằng `symbolCount` sau khi chu kỳ đầu tiên chạy xong
(xem log server, dòng `[Cycle] Xong - ...`).

# Chưa đổi (giữ nguyên chủ đích)

- Danh sách symbol chỉ tải 1 lần lúc start (không tự làm mới định kỳ) -
  đúng yêu cầu ban đầu. Nếu server chạy liên tục nhiều tuần, coin mới lên
  sàn trong thời gian đó sẽ KHÔNG được thêm vào tới khi restart server -
  chấp nhận được vì mục đích ban đầu, nhưng đáng để ý nếu sau này thấy
  thiếu coin mới list.
- Không có DB/lưu bền - mất cache khi restart, chấp nhận được vì app luôn
  có fallback gọi thẳng Binance khi server không trả dữ liệu.
