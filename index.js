const http = require('http');
const url = require('url');
const axios = require('axios');
const crypto = require('crypto');

// ==========================================
// 0. MÃ HOÁ RESPONSE - CÙNG CƠ CHẾ với máy chủ AI (`ai-machine-learning`,
// `AutoExportCodec` bên Flutter): XOR từng byte với khoá lặp vòng + base64,
// kèm chữ ký SHA-256 để app phát hiện dữ liệu bị sửa dọc đường. ĐÂY LÀ LÀM
// RỐI + XÁC THỰC, KHÔNG PHẢI MÃ HOÁ THẬT (khoá nằm ngay trong mã nguồn app,
// ai đọc file JS/APK đều lấy được) - chỉ chặn được người xem lướt qua tab
// Network, và giúp app CHẮC CHẮN dữ liệu tới từ đúng server này, chưa bị
// sửa dọc đường (MITM/proxy lạ). ĐỔI KHOÁ NÀY thì PHẢI đổi cả bên Flutter
// (`PriceHistoryCodec._key`), không thì app sẽ từ chối mọi phản hồi.
const RESPONSE_KEY = process.env.PRICE_HISTORY_KEY || 'sms_price_2026_qX9wRt';

function xorEncode(raw) {
  const data = Buffer.from(raw, 'utf8');
  const k = Buffer.from(RESPONSE_KEY, 'utf8');
  const out = Buffer.alloc(data.length);
  for (let i = 0; i < data.length; i++) out[i] = data[i] ^ k[i % k.length];
  return out.toString('base64');
}

function packResponse(dataObj) {
  const rawJson = JSON.stringify(dataObj);
  const sig = crypto.createHash('sha256').update(`${RESPONSE_KEY}|${rawJson}`).digest('hex');
  return { v: 1, payload: xorEncode(rawJson), sig };
}

// ==========================================
// 1. CẤU HÌNH
// ==========================================
const PORT = process.env.PORT || 8088;

// ⚠ BẢN SỬA: dùng ĐÚNG Futures (fapi), KHÔNG dùng Spot (api.binance.com) -
// app Flutter scan Futures USDT-M Perpetual, Spot có thể thiếu symbol/giá
// lệch (funding/basis) so với Futures.
const FUT_KLINES_URL = 'https://fapi.binance.com/fapi/v1/klines';
const FUT_EXCHANGE_INFO_URL = 'https://fapi.binance.com/fapi/v1/exchangeInfo';
const FUT_24HR_TICKER_URL = 'https://fapi.binance.com/fapi/v1/ticker/24hr';

// 6 khung app CẦN (khớp đúng `_filterMinutes()` bên Flutter - "24h" KHÔNG
// qua cơ chế này, app vẫn tự lấy 24h ticker riêng).
const WINDOW_TO_MINUTES = { '1m': 1, '5m': 5, '15m': 15, '30m': 30, '1h': 60, '4h': 240 };
const MAX_WINDOW_MINUTES = Math.max(...Object.values(WINDOW_TO_MINUTES)); // 240

// Số nến 1m lấy MỖI LƯỢT/symbol - đủ phủ khung XA NHẤT (4h = 240 nến) +
// đệm dư ra 10 nến (mạng chậm/nến chưa đóng kịp) - MỘT LẦN GỌI duy nhất suy
// ra được CẢ 6 khung, thay vì gọi riêng 6 lần/symbol như bản cũ.
const CANDLE_LIMIT = MAX_WINDOW_MINUTES + 10; // 250

// Refresh nến CHỦ ĐỘNG theo chu kỳ (KHÔNG đợi request tới mới fetch) - đúng
// yêu cầu "cứ lấy sẵn để đó". 90s (không phải 60s) - có biên an toàn hơn so
// với trần weight Binance (xem tính toán ở dưới).
const REFRESH_INTERVAL_MS = 90 * 1000;

// Gọi theo lô, nghỉ giữa lô - dịu tải, tránh dội 429. Với danh sách ~450
// symbol, batch 20 + nghỉ 100ms -> 1 chu kỳ mất khoảng 10-15s, XONG TRƯỚC
// chu kỳ tiếp theo rất nhiều (90s) - không bao giờ chồng lấn 2 chu kỳ.
const BATCH_SIZE = 20;
const BATCH_DELAY_MS = 100;

// ==========================================
// TÍNH TOÁN NGÂN SÁCH REQUEST (ghi lại để lần sau đổi số dễ đối chiếu)
// ==========================================
// limit=250 -> weight Binance = 2/request (limit 100-500 dải weight=2).
// ~450 symbol x 1 request/chu kỳ x weight 2 = ~900 weight/chu kỳ.
// Trần Binance Futures: 2400 weight/phút. Chu kỳ 90s -> ngân sách còn RẤT
// RỘNG (900 << 2400), khác hẳn bản cũ (6 request/symbol -> ~2700 weight/
// chu kỳ 60s, VƯỢT trần).

// ==========================================
// STATE (RAM) - không dùng DB, mất khi restart là CHẤP NHẬN ĐƯỢC (app luôn
// có fallback gọi thẳng Binance nếu server này không trả dữ liệu).
// ==========================================
/** @type {string[]} */
let symbolList = []; // CHỈ tải 1 LẦN lúc start (đúng yêu cầu), không tự làm mới định kỳ.
let symbolListLoadedAt = null;

/** Map<symbol, {timestamp:number, candles: Array<[openTime,open,high,low,close,...]>}> */
const candleCache = new Map();
let lastCycleStartedAt = null;
let lastCycleFinishedAt = null;
let cycleInProgress = false;

// ==========================================
// 2. LẤY DANH SÁCH SYMBOL - Y HỆT LOGIC APP FLUTTER (`explore_screen.dart`
// `_allSymbols`): GIAO giữa exchangeInfo (USDT+PERPETUAL+TRADING) VÀ có mặt
// trong 24hr ticker (Explore lọc vậy - loại coin vừa list/chưa có đủ dữ
// liệu 24h) - KHÔNG áp thêm `excludeKeywords` (USDC/BUSD/UP/DOWN...), vì
// Explore (nơi server này phục vụ) không lọc phần đó.
// ==========================================
async function loadSymbolList() {
  const [exInfoRes, tickerRes] = await Promise.all([
    axios.get(FUT_EXCHANGE_INFO_URL, { timeout: 10000 }),
    axios.get(FUT_24HR_TICKER_URL, { timeout: 10000 }),
  ]);

  const perpetualSymbols = (exInfoRes.data.symbols || [])
    .filter((s) => s.quoteAsset === 'USDT' && s.contractType === 'PERPETUAL' && s.status === 'TRADING')
    .map((s) => s.symbol);

  const tickerSymbols = new Set((tickerRes.data || []).map((t) => t.symbol));

  return perpetualSymbols.filter((s) => tickerSymbols.has(s));
}

// ==========================================
// 3. LẤY NHIỀU NẾN 1M CHO 1 SYMBOL (1 REQUEST DUY NHẤT, đủ phủ mọi khung)
// ==========================================
async function fetchCandlesForSymbol(symbol) {
  try {
    const response = await axios.get(FUT_KLINES_URL, {
      params: { symbol, interval: '1m', limit: CANDLE_LIMIT },
      timeout: 8000,
    });
    if (Array.isArray(response.data) && response.data.length > 0) {
      return response.data;
    }
    return null;
  } catch (error) {
    return null;
  }
}

// ==========================================
// 4. LÀM MỚI TOÀN BỘ CACHE THEO LÔ (chạy nền, KHÔNG chờ request nào)
// ==========================================
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function refreshAllCandles() {
  if (cycleInProgress) return;

  // BUG ĐÃ SỬA (người dùng phát hiện qua log thật): TRƯỚC ĐÂY nếu
  // `symbolList` rỗng (VD lỗi lúc khởi động - 418 do dính rate-limit/ban
  // tạm thời từ Binance ngay lúc server vừa lên) thì hàm này ÂM THẦM
  // `return` ở đây MÃI MÃI - dù `setInterval` vẫn gọi lại đều đặn mỗi 90s,
  // không có lấy 1 dòng log, KHÔNG CÓ CƠ CHẾ THỬ LẠI nào cả - server sống
  // (Express vẫn chạy) nhưng phần lấy giá coi như CHẾT HẲN tới khi có người
  // tự khởi động lại server. Giờ THỬ TẢI LẠI danh sách symbol NGAY TẠI ĐÂY
  // mỗi khi thấy rỗng - vì hàm này vốn đã được gọi lại đều đặn mỗi 90s qua
  // `setInterval`, tự nhiên có được nhịp thử lại KHÔNG CẦN thêm cơ chế
  // backoff riêng (90s giữa các lần thử là đủ giãn cách, không tính là dồn
  // dập lên Binance - chỉ 2 request nhẹ/lần thử, không phải hàng trăm).
  if (symbolList.length === 0) {
    try {
      symbolList = await loadSymbolList();
      symbolListLoadedAt = Date.now();
      console.log(`[Symbols] Thử lại THÀNH CÔNG - đã tải ${symbolList.length} symbol.`);
    } catch (e) {
      console.error('[Symbols] Thử lại vẫn lỗi:', e.message, '- sẽ thử lại ở chu kỳ 90s sau.');
      return; // vẫn rỗng - KHÔNG chạy phần nến, đợi lần gọi kế tiếp.
    }
  }

  cycleInProgress = true;
  lastCycleStartedAt = Date.now();

  for (let i = 0; i < symbolList.length; i += BATCH_SIZE) {
    const batch = symbolList.slice(i, i + BATCH_SIZE);
    await Promise.all(
      batch.map(async (symbol) => {
        const candles = await fetchCandlesForSymbol(symbol);
        // Cập nhật cache NGAY khi symbol này xong - không đợi cả lô/cả chu
        // kỳ hoàn tất - symbol nào xong sớm thì có dữ liệu mới sớm.
        if (candles) {
          candleCache.set(symbol, { timestamp: Date.now(), candles });
        }
        // Lỗi 1 symbol (mạng/429/delist giữa chừng) -> GIỮ NGUYÊN cache CŨ
        // của symbol đó (nếu có) thay vì xoá - "dữ liệu hơi cũ" luôn tốt
        // hơn "không có gì", app vẫn tự fallback Binance nếu thấy thiếu.
      })
    );
    if (i + BATCH_SIZE < symbolList.length) await sleep(BATCH_DELAY_MS);
  }

  lastCycleFinishedAt = Date.now();
  cycleInProgress = false;
  console.log(
    `[Cycle] Xong - ${candleCache.size}/${symbolList.length} symbol có cache, mất ${
      lastCycleFinishedAt - lastCycleStartedAt
    }ms`
  );
}

// ==========================================
// 5. SUY RA GIÁ TẠI 1 MỐC LÙI (phút) TỪ CACHE NẾN CỦA 1 SYMBOL - THAY VÌ
// GỌI BINANCE RIÊNG CHO TỪNG KHUNG.
// ==========================================
function priceAtMinutesAgo(candles, minutesAgo) {
  if (!candles || candles.length === 0) return null;
  const idx = candles.length - 1 - minutesAgo;
  if (idx < 0) return null;
  return parseFloat(candles[idx][4]); // index 4 = giá đóng cửa (close)
}

// ==========================================
// 6. HTTP SERVER
// ==========================================
const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const parsedUrl = url.parse(req.url, true);
  const pathname = parsedUrl.pathname;
  const query = parsedUrl.query;

  // ---- /api/history_prices?window=4h ----
  if (pathname === '/api/history_prices') {
    const windowParam = query.window || '4h';
    const minutesAgo = WINDOW_TO_MINUTES[windowParam];

    // Whitelist rõ ràng - khung lạ thì báo lỗi ngay, không dò cache vô ích.
    if (minutesAgo === undefined) {
      res.writeHead(400);
      return res.end(
        JSON.stringify({
          status: 'error',
          message: `window không hợp lệ - chỉ nhận: ${Object.keys(WINDOW_TO_MINUTES).join(', ')}`,
        })
      );
    }

    // Symbol quá cũ (kẹt lỗi liên tục nhiều chu kỳ, cache không được làm
    // mới) -> BỎ QUA, coi như "không có" (giống hệt nhánh `!cached` phía
    // trên) - để app tự fallback ĐÚNG những symbol lỗi đó qua đường
    // Binance-trực-tiếp sẵn có, KHÔNG kéo cả response (~500 symbol khác vẫn
    // tươi bình thường) xuống theo. BUG ĐÃ SỬA (người dùng phát hiện qua
    // DevTools: server luôn trả data mới, nhưng Flutter cứ fallback) -
    // TRƯỚC ĐÂY tính "mốc CŨ NHẤT trong toàn bộ response" rồi gửi cho app
    // tự so ngưỡng - chỉ CẦN ĐÚNG 1 symbol kẹt lỗi (VD coin mới delist) là
    // kéo mốc đó cũ MÃI MÃI, dù 499 symbol còn lại tươi hoàn toàn - khiến
    // app fallback TOÀN BỘ oan uổng. Giờ lọc NGAY TẠI ĐÂY, per-symbol,
    // không còn "1 con sâu làm rầu nồi canh" nữa.
    const STALE_SYMBOL_MS = 10 * 60 * 1000; // 10 phút - rộng hơn TTL 90s nhiều lần

    const data = {};
    const now = Date.now();
    for (const symbol of symbolList) {
      const cached = candleCache.get(symbol);
      if (!cached) continue; // chưa kịp có cache (mới start) - app tự fallback Binance
      if (now - cached.timestamp > STALE_SYMBOL_MS) continue; // symbol này kẹt lỗi lâu - bỏ qua riêng nó
      const price = priceAtMinutesAgo(cached.candles, minutesAgo);
      if (price !== null) data[symbol] = price;
    }

    // CHỈ mã hoá response THÀNH CÔNG (có data thật) - lỗi (400 ở trên) giữ
    // JSON thường, đúng quy ước bên máy chủ AI (dễ debug hơn, không có gì
    // nhạy cảm cần bảo vệ ở thông báo lỗi).
    res.writeHead(200);
    return res.end(
      JSON.stringify(
        packResponse({
          status: 'success',
          source: 'CANDLE_CACHE',
          window: windowParam,
          symbolCount: symbolList.length,
          cachedCount: Object.keys(data).length,
          // MỚI (đã sửa lại): SỨC KHOẺ CẢ CHU KỲ REFRESH NỀN - `null` nếu
          // CHƯA từng có 1 chu kỳ nào chạy xong (mới start). Đây là chỉ số
          // TOÀN CỤC, không lệ thuộc số phận của 1 symbol lẻ nào - app dùng
          // để tự phát hiện "vòng lặp nền có còn sống không" (VD dính
          // rate-limit Binance liên tục khiến CẢ CHU KỲ không xong nổi
          // trong thời gian dài) mà không sợ 1 symbol lỗi đơn lẻ báo động
          // giả (đã lọc riêng ở trên rồi).
          lastCycleFinishedAtMs: lastCycleFinishedAt,
          data,
        })
      )
    );
  }

  // ---- /health - kiểm tra nhanh server đã sẵn sàng chưa ----
  if (pathname === '/health') {
    res.writeHead(200);
    return res.end(
      JSON.stringify({
        status: 'ok',
        symbolListLoadedAt,
        symbolCount: symbolList.length,
        cachedSymbolCount: candleCache.size,
        cycleInProgress,
        lastCycleStartedAt,
        lastCycleFinishedAt,
        lastCycleDurationMs:
          lastCycleFinishedAt && lastCycleStartedAt ? lastCycleFinishedAt - lastCycleStartedAt : null,
      })
    );
  }

  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Smart Money Scanner Price API is running!');
});

// ==========================================
// 7. KHỞI ĐỘNG
// ==========================================
async function start() {
  // Mở cổng NGAY (Render/health-check cần cổng sống sớm) - danh sách symbol
  // + cache nến tải SONG SONG phía sau, endpoint tự trả rỗng cho tới khi có
  // dữ liệu (app luôn có fallback Binance nên không sao).
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`[Server] Running on port ${PORT}`);
  });

  try {
    symbolList = await loadSymbolList();
    symbolListLoadedAt = Date.now();
    console.log(`[Symbols] Đã tải ${symbolList.length} symbol (USDT-PERPETUAL, có trong 24hr ticker).`);
  } catch (e) {
    console.error('[Symbols] Lỗi tải danh sách symbol lúc start:', e.message);
    // KHÔNG crash server - BUG ĐÃ SỬA: TRƯỚC ĐÂY comment ở đây ghi "tự thử
    // lại ở chu kỳ refresh sau" nhưng THỰC RA CHƯA LÀM - `refreshAllCandles()`
    // cũ chỉ `return` im lặng nếu `symbolList` rỗng, KHÔNG thử tải lại bao
    // giờ (server sống nhưng phần lấy giá coi như chết hẳn tới khi restart
    // thủ công). Giờ ĐÃ SỬA THẬT trong `refreshAllCandles()` - mỗi lần gọi
    // (kể cả từ `setInterval` mỗi 90s bên dưới) đều tự thử `loadSymbolList()`
    // lại nếu đang rỗng, có log rõ ràng cho cả 2 trường hợp thành công/vẫn lỗi.
  }

  // Chu kỳ ĐẦU TIÊN chạy ngay (không đợi đủ 90s mới có data lần đầu).
  refreshAllCandles();
  setInterval(refreshAllCandles, REFRESH_INTERVAL_MS);
}

start();
