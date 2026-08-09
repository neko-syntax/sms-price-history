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
// ĐÃ BỎ proxy (xoay vòng nhiều IP) - KHÔNG còn cần nữa: giờ chạy trên VPS
// riêng, IP DEDICATED (không dùng chung với ai khác) - rủi ro "dính ban lây
// từ người khác" mà proxy dùng chung từng gặp không còn áp dụng ở đây.

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
// MỚI: MARKET CAP (gộp từ server riêng trước đây - dùng CHUNG 1 tiến trình,
// 1 cổng với price-history, ÁP DỤNG ĐÚNG các nguyên tắc đã chứng minh hoạt
// động tốt ở phần nến: watchdog chống treo, log rõ ràng, không im lặng, TTL
// hợp lý theo đúng NHỊP ĐỔI THẬT của dữ liệu (marketcap không đổi nhanh như
// giá nến - giữ TTL 2 tiếng như bản gốc, KHÁC 90s của nến - đây là TTL ĐÚNG
// cho marketcap, không phải "chưa tối ưu").
// ==========================================
const COINGECKO_API_KEY = process.env.COINGECKO_API_KEY || 'CG-vQyeAManc8vSngMb1TfPpp88';
const MC_REFRESH_INTERVAL_MS = 2 * 60 * 60 * 1000; // 2 tiếng - khớp nhịp đổi thật của vốn hoá.
// Watchdog CHO CẢ VÒNG CÀO (giống hệt lý do đã áp dụng cho nến - vòng lặp
// gọi CoinGecko nhiều trang liên tiếp CŨNG là 1 chuỗi request có thể treo ở
// tầng TCP/DNS mà `fetch` không luôn tự huỷ được, cùng LOẠI RỦI RO đã từng
// gây bug "câm lặng" cho phần nến trước khi có watchdog - áp dụng NGAY TỪ
// ĐẦU cho marketcap, không đợi tự dính bug rồi mới vá).
const MC_CYCLE_WATCHDOG_MS = 5 * 60 * 1000; // rộng rãi cho vài trăm trang CoinGecko.
const MC_PAGE_DELAY_MS = 250; // giữ nguyên nhịp nghỉ giữa trang như bản gốc.

/** @type {Record<string, number>} symbol GỐC (không hậu tố USDT, không prefix nhân hệ số) -> market cap USD */
let marketCapCache = {};
let mcLastUpdated = null;
let mcCycleInProgress = false;
let mcLastCycleStartedAt = null;
let mcLastCycleFinishedAt = null;

/// Quy đổi symbol kiểu Binance FUTURES (VD "1000PEPEUSDT", "BTCUSDT") về
/// symbol GỐC dùng làm khoá trong `marketCapCache` (VD "PEPE", "BTC") - bỏ
/// hậu tố "USDT" + bỏ tiền tố SỐ nhân hệ số (1000/10000/1000000...) Binance
/// hay gắn trước 1 số coin nhỏ giá trị (VD 1000SHIB = hợp đồng đại diện cho
/// 1000 SHIB thật). GIỚI HẠN ĐÃ BIẾT: không xử lý dạng tiền tố CHỮ+SỐ hiếm
/// gặp kiểu "1M..." (nếu Binance có coin dạng này) - CHỈ xử lý tiền tố SỐ
/// THUẦN, đủ dùng cho tuyệt đại đa số symbol thực tế hiện có.
function normalizeToBaseSymbol(rawSymbol) {
  let s = String(rawSymbol || '').toUpperCase().trim();
  if (s.endsWith('USDT')) s = s.slice(0, -4);
  s = s.replace(/^\d+/, ''); // bỏ tiền tố số đứng đầu (1000, 10000, 1000000...)
  return s;
}

/// Tra market cap cho 1 symbol Futures BẤT KỲ (tự quy đổi prefix/hậu tố) -
/// dùng cho endpoint `/api/mc/:symbol` - trả `null` nếu không tìm thấy.
function lookupMarketCap(rawSymbol) {
  const base = normalizeToBaseSymbol(rawSymbol);
  if (!base || !(base in marketCapCache)) return null;
  return { symbol: base, marketCap: marketCapCache[base] };
}

/// Cào TOÀN BỘ market cap từ CoinGecko (Binance USDT tickers, phân trang) -
/// PORT lại từ server marketcap cũ, THÊM watchdog bảo vệ cả vòng cào (xem
/// giải thích ở khai báo `MC_CYCLE_WATCHDOG_MS`) + log nhất quán kiểu
/// `[MC-Cycle]` (song song `[Cycle]` của phần nến, dễ phân biệt trong log
/// chung 1 tiến trình).
async function updateMarketCapCache() {
  if (mcCycleInProgress) {
    const stuckMs = mcLastCycleStartedAt ? Date.now() - mcLastCycleStartedAt : null;
    console.warn(`[MC-Cycle] Chu kỳ trước vẫn đang chạy (bắt đầu ${stuckMs}ms trước) - bỏ qua lần gọi này.`);
    return;
  }

  mcCycleInProgress = true;
  mcLastCycleStartedAt = Date.now();
  console.log(`[MC-Cycle] Bắt đầu cào market cap từ CoinGecko...`);

  const tempMap = {};
  let pageCount = 0;

  try {
    await Promise.race([
      (async () => {
        let page = 1;
        while (true) {
          const mcUrl = `https://api.coingecko.com/api/v3/exchanges/binance/tickers?order=volume_desc&page=${page}`;
          const response = await fetch(mcUrl, { headers: { 'x-cg-demo-api-key': COINGECKO_API_KEY } });

          if (!response.ok) {
            console.error(`[MC-Cycle] Trang ${page} trả lỗi HTTP: ${response.status} - dừng vòng cào.`);
            break;
          }

          const data = await response.json();
          const tickers = Array.isArray(data?.tickers) ? data.tickers : [];
          if (tickers.length === 0) {
            pageCount = page - 1;
            break;
          }

          for (const item of tickers) {
            if (
              typeof item?.target === 'string' &&
              item.target.toUpperCase() === 'USDT' &&
              item.base &&
              typeof item.coin_mcap_usd === 'number'
            ) {
              const symbol = item.base.toUpperCase();
              // Ưu tiên giữ giá trị đầu tiên gặp (volume cao nhất, đúng thứ
              // tự `order=volume_desc` của CoinGecko).
              if (!(symbol in tempMap)) tempMap[symbol] = item.coin_mcap_usd;
            }
          }

          page++;
          await sleep(MC_PAGE_DELAY_MS);
        }
      })(),
      sleep(MC_CYCLE_WATCHDOG_MS).then(() => {
        throw new Error(`MC_CYCLE_WATCHDOG_TIMEOUT sau ${MC_CYCLE_WATCHDOG_MS}ms - có request treo bất thường`);
      }),
    ]);

    const totalCount = Object.keys(tempMap).length;
    if (totalCount > 0) {
      marketCapCache = tempMap;
      mcLastUpdated = new Date().toISOString();
      mcLastCycleFinishedAt = Date.now();
      console.log(
        `[MC-Cycle] Xong - ${totalCount} coin USDT, ${pageCount} trang, mất ${
          mcLastCycleFinishedAt - mcLastCycleStartedAt
        }ms.`
      );
    } else {
      console.warn('[MC-Cycle] Không lấy được dữ liệu mới (0 coin) - giữ nguyên cache cũ, không ghi đè.');
    }
  } catch (e) {
    // Watchdog cắt ngang HOẶC lỗi mạng thật sự - LUÔN log rõ (đúng nguyên
    // tắc "dù lý do gì cũng phải log" đã áp dụng cho phần nến) - GIỮ
    // NGUYÊN cache cũ (nếu có), không xoá - "dữ liệu hơi cũ" vẫn tốt hơn
    // "không có gì".
    console.error(
      `[MC-Cycle] LỖI/TREO giữa chừng: ${e.message} - giữ nguyên cache cũ (${
        Object.keys(marketCapCache).length
      } coin).`
    );
  } finally {
    // LUÔN reset - dù thành công/lỗi/watchdog cắt - chu kỳ SAU (2 tiếng
    // tới) chắc chắn được thử lại, không bao giờ kẹt vĩnh viễn (đúng bài
    // học đã áp dụng cho phần nến).
    mcCycleInProgress = false;
  }
}

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

// MỚI: cơ chế "nghỉ" khi phát hiện bị Binance ban diện rộng (418/429) - xem
// giải thích đầy đủ ở chỗ dùng (`refreshAllCandles`). KHÔNG dùng DB, mất
// khi restart cũng KHÔNG SAO (restart giữa lúc bị ban thì request đầu tiên
// sau restart sẽ lại phát hiện ban ngay, tự set lại cooldown bình thường).
let banCooldownUntil = 0;
let consecutiveBanCycles = 0;

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
async function fetchCandlesForSymbol(symbol, failures) {
  try {
    const response = await axios.get(FUT_KLINES_URL, {
      params: { symbol, interval: '1m', limit: CANDLE_LIMIT },
      timeout: 8000,
    });
    if (Array.isArray(response.data) && response.data.length > 0) {
      return response.data;
    }
    // ĐÃ SỬA (người dùng yêu cầu: "dù lý do gì cũng phải log"): TRƯỚC ĐÂY
    // response rỗng/không đúng dạng bị nuốt HOÀN TOÀN im lặng, coi như lỗi
    // mạng bình thường - giờ ghi lại lý do CỤ THỂ vào mảng `failures` (dùng
    // chung 1 chỗ với nhánh catch bên dưới) - `refreshAllCandles()` sẽ tự
    // log tóm tắt cuối chu kỳ, không cần log riêng ở đây (tránh ngập log với
    // 450 symbol).
    if (failures) failures.push({ symbol, reason: 'empty_response' });
    return null;
  } catch (error) {
    if (failures) {
      // Phân biệt rõ 429/418 (rate-limit/ban - ĐÁNG LO, cần biết ngay) với
      // lỗi mạng/timeout thường (symbol lẻ tẻ, không đáng lo).
      const status = error.response?.status;
      const reason = status ? `HTTP_${status}` : error.code || error.message || 'unknown';
      // MỚI: Binance LUÔN kèm header `Retry-After` (số giây) khi trả
      // 418/429 - có ca còn kèm mốc thời gian TUYỆT ĐỐI hết ban
      // (`data.data.retryAfter`, epoch ms) ngay trong nội dung lỗi. Ưu
      // tiên đọc mốc tuyệt đối (chính xác hơn - không lệch dù server xử lý
      // chậm 1 chút), fallback qua header dạng số giây nếu không có.
      let banUntilMs = null;
      if (status === 418 || status === 429) {
        const bodyRetryAfterMs = error.response?.data?.data?.retryAfter;
        const retryAfterHeaderSec = error.response?.headers?.['retry-after'];
        if (typeof bodyRetryAfterMs === 'number') {
          banUntilMs = bodyRetryAfterMs;
        } else if (retryAfterHeaderSec) {
          banUntilMs = Date.now() + Number(retryAfterHeaderSec) * 1000;
        } else {
          // Binance không kèm mốc rõ ràng lần này - tự đặt 1 khoảng nghỉ AN
          // TOÀN mặc định (2 phút - đúng mức BAN NGẮN NHẤT theo tài liệu
          // Binance).
          banUntilMs = Date.now() + 2 * 60 * 1000;
        }
      }
      failures.push({ symbol, reason, banUntilMs });
    }
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
  if (cycleInProgress) {
    // ĐÃ SỬA (người dùng chỉ ra: guard này TRƯỚC ĐÂY 100% câm lặng): log rõ
    // thay vì im lặng - nếu dòng này lặp lại NHIỀU lần liên tiếp (mỗi 90s),
    // nghĩa là chu kỳ trước đang bị TREO (xem watchdog bên dưới - dù treo
    // thật thì cũng chỉ tối đa vài lần trước khi watchdog tự cắt).
    const stuckMs = lastCycleStartedAt ? Date.now() - lastCycleStartedAt : null;
    console.warn(
      `[Cycle] Chu kỳ trước vẫn đang chạy (bắt đầu ${stuckMs}ms trước) - bỏ qua lần gọi này.`
    );
    return;
  }

  // Đang trong thời gian "nghỉ" do chu kỳ TRƯỚC phát hiện ban diện rộng -
  // bỏ qua HẲN, KHÔNG gửi thêm request nào cả. LÝ DO: gửi tiếp trong lúc
  // đang bị Binance ban (418) có thể khiến họ GIA HẠN thời gian ban lâu hơn
  // (theo đúng chính sách chống spam của họ), dù request có gửi đúng nhịp/
  // không dồn dập tới đâu - vì lúc này KHÔNG PHẢI vấn đề tốc độ, mà là IP đã
  // bị đưa vào danh sách chặn, chỉ có CHỜ mới hết.
  if (Date.now() < banCooldownUntil) {
    console.warn(
      `[Cycle] Đang NGHỈ do nghi ngờ bị Binance ban (còn ${Math.ceil(
        (banCooldownUntil - Date.now()) / 1000
      )}s) - bỏ qua, không gửi thêm request nào để tránh bị gia hạn ban.`
    );
    return;
  }

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

  // MỚI - WATCHDOG CHO CẢ CHU KỲ (khắc phục bug "câm lặng, không lấy được
  // data" người dùng phát hiện): TRƯỚC ĐÂY không có giới hạn thời gian nào
  // cho toàn bộ vòng lặp lấy nến - chỉ TỪNG request riêng có `timeout: 8000`
  // (bên trong `fetchCandlesForSymbol`). Nhưng `timeout` của axios KHÔNG
  // đảm bảo kích hoạt 100% mọi tình huống (có ca hiếm treo ở tầng TCP/DNS mà
  // timeout không bắt được - vấn đề đã biết của axios/Node, không phải suy
  // đoán). Chỉ cần ĐÚNG 1 trong hàng trăm request rơi vào ca đó:
  // `Promise.all()` của lô đó đứng hình VĨNH VIỄN -> `cycleInProgress`
  // không bao giờ về lại `false` -> KHÔNG throw, KHÔNG catch được, KHÔNG
  // log gì cả (không lỗi, chỉ treo im lặng) -> mọi lần gọi `setInterval` sau
  // đó bị chặn âm thầm MÃI MÃI qua guard ở đầu hàm. `Promise.race` với 1
  // watchdog timeout CỨNG cho CẢ CHU KỲ đảm bảo dù có request nào treo kiểu
  // gì, hàm này CHẮC CHẮN thoát ra được (qua `finally`), log rõ lý do, và
  // chu kỳ 90s SAU vẫn được thử lại bình thường - không bao giờ kẹt vĩnh
  // viễn nữa.
  const CYCLE_WATCHDOG_MS = 60 * 1000; // rộng hơn nhiều so với ~10-15s bình thường
  // Đếm lỗi từng symbol để LOG TÓM TẮT cuối chu kỳ (người dùng yêu cầu:
  // "dù lý do gì cũng phải log" - nhưng log riêng lẻ từng symbol lỗi trong
  // 450 symbol sẽ ngập log vô ích, nên gom lại thành 1 dòng tóm tắt).
  const failures = [];

  try {
    await Promise.race([
      (async () => {
        for (let i = 0; i < symbolList.length; i += BATCH_SIZE) {
          const batch = symbolList.slice(i, i + BATCH_SIZE);
          await Promise.all(
            batch.map(async (symbol) => {
              const candles = await fetchCandlesForSymbol(symbol, failures);
              // Cập nhật cache NGAY khi symbol này xong - không đợi cả lô/cả
              // chu kỳ hoàn tất - symbol nào xong sớm thì có dữ liệu mới sớm.
              if (candles) {
                candleCache.set(symbol, { timestamp: Date.now(), candles });
              }
              // Lỗi 1 symbol (mạng/429/delist giữa chừng) -> GIỮ NGUYÊN cache
              // CŨ của symbol đó (nếu có) thay vì xoá - "dữ liệu hơi cũ" luôn
              // tốt hơn "không có gì", app vẫn tự fallback Binance nếu thiếu.
            })
          );
          if (i + BATCH_SIZE < symbolList.length) await sleep(BATCH_DELAY_MS);
        }
      })(),
      sleep(CYCLE_WATCHDOG_MS).then(() => {
        throw new Error(`CYCLE_WATCHDOG_TIMEOUT sau ${CYCLE_WATCHDOG_MS}ms - có request treo bất thường`);
      }),
    ]);

    lastCycleFinishedAt = Date.now();
    // MỚI: log tóm tắt lỗi (nếu có) NGAY TRONG dòng log hoàn tất - đúng yêu
    // cầu "dù lý do gì cũng phải log" mà không ngập log 450 dòng riêng lẻ.
    const failSummary =
      failures.length === 0
        ? ''
        : ` | ${failures.length} symbol lỗi (VD: ${failures
            .slice(0, 5)
            .map((f) => `${f.symbol}:${f.reason}`)
            .join(', ')}${failures.length > 5 ? '...' : ''})`;
    console.log(
      `[Cycle] Xong - ${candleCache.size}/${symbolList.length} symbol có cache, mất ${
        lastCycleFinishedAt - lastCycleStartedAt
      }ms${failSummary}`
    );

    // MỚI: cảnh báo RIÊNG, NỔI BẬT nếu nghi ngờ bị Binance rate-limit/ban
    // DIỆN RỘNG (429/418 chiếm phần lớn số lỗi) - đúng kịch bản đã từng gây
    // bug "câm lặng" trước đây (lúc đó xảy ra ngay lúc start nên có log
    // riêng; giờ thêm để bắt được cả khi nó xảy ra GIỮA CHỪNG lúc server đã
    // chạy lâu, trước đây hoàn toàn không có cảnh báo nào cho ca này).
    const banLikeCount = failures.filter(
      (f) => f.reason === 'HTTP_429' || f.reason === 'HTTP_418'
    ).length;
    const banRatio = symbolList.length > 0 ? banLikeCount / symbolList.length : 0;
    if (banLikeCount >= 20) {
      // MỚI: nếu Binance có kèm mốc hết ban, log ra CHÍNH XÁC còn bao lâu -
      // đỡ phải mù mờ đoán "cứ thử lại mỗi 90s tới khi nào thì thôi".
      const banUntilMs = failures.find((f) => f.banUntilMs)?.banUntilMs;
      const banInfo = banUntilMs
        ? ` - Binance báo hết ban lúc ${new Date(banUntilMs).toLocaleString('vi-VN', {
            hour12: false,
          })} (còn ~${Math.max(0, Math.ceil((banUntilMs - Date.now()) / 1000))}s)`
        : ' - Binance không kèm mốc hết ban trong response lần này.';
      console.warn(
        `[Cycle] ⚠ NGHI NGỜ ĐANG BỊ BINANCE RATE-LIMIT/BAN DIỆN RỘNG - ${banLikeCount}/${symbolList.length} symbol trả 429/418 trong chu kỳ này${banInfo}`
      );
    }

    // Quá nửa symbol bị 429/418 -> gần như chắc chắn CẢ IP đang bị ban,
    // không phải lỗi lẻ tẻ - kích hoạt "nghỉ" (xem giải thích đầy đủ ở đầu
    // hàm, chỗ check `banCooldownUntil`). TĂNG DẦN thời gian nghỉ nếu ban
    // LẶP LẠI nhiều chu kỳ liên tiếp (5 phút -> 10 phút -> ... tối đa 30
    // phút) - tự thích nghi với ban ngắn lẫn ban dài, không cần biết trước
    // Binance ban bao lâu.
    if (banRatio >= 0.5) {
      consecutiveBanCycles++;
      const backoffMs = Math.min(5 * 60 * 1000 * consecutiveBanCycles, 30 * 60 * 1000);
      banCooldownUntil = Date.now() + backoffMs;
      console.warn(
        `[Cycle] ⚠ BAN DIỆN RỘNG (${banLikeCount}/${symbolList.length}, lần liên tiếp thứ ${consecutiveBanCycles}) - TẠM NGHỈ ${
          backoffMs / 1000
        }s trước khi thử lại, không gửi thêm request trong lúc này.`
      );
    } else if (consecutiveBanCycles > 0) {
      console.log(`[Cycle] Hết ban (tỉ lệ lỗi 429/418 đã giảm) - reset bộ đếm nghỉ.`);
      consecutiveBanCycles = 0;
    }
  } catch (e) {
    // Watchdog cắt ngang (hoặc lỗi thật sự chưa lường trước) - LUÔN log rõ,
    // không để im lặng như trước. LƯU Ý: watchdog KHÔNG hủy được request
    // đang treo thật sự (Node không có cách "giết" 1 Promise đang chờ) -
    // request đó vẫn tự chạy ngầm rồi tự kết thúc sau, nhưng quan trọng nhất
    // là chu kỳ SAU không còn bị nó chặn nữa (nhờ `finally` bên dưới).
    console.error(
      `[Cycle] LỖI/TREO giữa chừng: ${e.message} - đã có ${candleCache.size}/${symbolList.length} symbol trong cache trước đó (giữ nguyên).`
    );
  } finally {
    // LUÔN reset - dù thành công, lỗi, hay bị watchdog buộc dừng - để chu
    // kỳ SAU (90s tới, qua setInterval) CHẮC CHẮN được thử lại, không bao
    // giờ kẹt vĩnh viễn nữa. Đây là dòng SỬA QUAN TRỌNG NHẤT cho bug này.
    cycleInProgress = false;
  }
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
    // MỚI (theo yêu cầu bảo mật): response PUBLIC ra ngoài LUÔN tiếng Anh,
    // KHÔNG gợi ý giá trị hợp lệ là gì - ai gọi sai hẳn không phải app thật
    // (app thật luôn gửi đúng), gợi ý chỉ giúp dò qué/probe API dễ hơn. Log
    // console (chỉ admin đọc) vẫn giữ tiếng Việt như mọi log khác trong file.
    if (minutesAgo === undefined) {
      console.warn(`[history_prices] window không hợp lệ: "${windowParam}"`);
      res.writeHead(400);
      return res.end(JSON.stringify({ status: 'error', message: 'Invalid request.' }));
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

  // ---- /api/market-caps - TOÀN BỘ market cap (đã gộp từ server riêng
  // trước đây) - dùng khi FLUTTER cần tự so khớp/xử lý prefix Futures
  // (1000/10000/1000000...) phía client, đỡ phải gọi lẻ từng coin. ----
  if (pathname === '/api/market-caps') {
    res.writeHead(200);
    return res.end(
      JSON.stringify(
        packResponse({
          status: 'success',
          last_updated: mcLastUpdated,
          total: Object.keys(marketCapCache).length,
          data: marketCapCache,
        })
      )
    );
  }

  // ---- /api/mc/:symbol HOẶC /api/mc?symbol=:symbol - market cap ĐÚNG 1
  // coin (nhẹ hơn get-all) - hỗ trợ CẢ 2 kiểu gọi, ưu tiên path segment nếu
  // có. Tự quy đổi prefix/hậu tố Futures (xem `lookupMarketCap`) - endpoint
  // này TỰ xử lý luôn, không bắt caller phải biết trước quy tắc. ----
  if (pathname === '/api/mc' || pathname.startsWith('/api/mc/')) {
    const pathSymbol = pathname.startsWith('/api/mc/') ? pathname.slice('/api/mc/'.length) : '';
    const rawSymbol = decodeURIComponent(pathSymbol || query.symbol || '').trim();

    // MỚI (theo yêu cầu bảo mật - ÁP DỤNG CHUNG cho cả file, xem giải thích
    // ở endpoint `/api/history_prices` phía trên): response PUBLIC LUÔN
    // tiếng Anh, KHÔNG gợi ý cách gọi đúng - console log (admin-only) mới
    // giữ chi tiết bằng tiếng Việt.
    if (!rawSymbol) {
      console.warn('[mc] Request thiếu symbol.');
      res.writeHead(400);
      return res.end(JSON.stringify({ status: 'error', message: 'Invalid request.' }));
    }

    const found = lookupMarketCap(rawSymbol);
    if (!found) {
      console.warn(`[mc] Không tìm thấy market cap cho "${rawSymbol}".`);
      res.writeHead(404);
      return res.end(JSON.stringify({ status: 'error', message: 'Not found.' }));
    }

    res.writeHead(200);
    return res.end(
      JSON.stringify(
        packResponse({
          status: 'success',
          last_updated: mcLastUpdated,
          query_symbol: rawSymbol,
          matched_symbol: found.symbol,
          market_cap: found.marketCap,
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
        // MỚI: thêm sức khoẻ chu kỳ market cap - dùng ĐÚNG mẫu tên field
        // (tiền tố "mc") song song với phần nến ở trên, dễ đối chiếu.
        mcCoinCount: Object.keys(marketCapCache).length,
        mcLastUpdated,
        mcCycleInProgress,
        mcLastCycleStartedAt,
        mcLastCycleFinishedAt,
        mcLastCycleDurationMs:
          mcLastCycleFinishedAt && mcLastCycleStartedAt ? mcLastCycleFinishedAt - mcLastCycleStartedAt : null,
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

  // MỚI: market cap - chu kỳ ĐẦU TIÊN chạy ngay (fire-and-forget, KHÔNG
  // `await` - không lý do gì chặn `start()`/cổng HTTP chờ vòng cào CoinGecko
  // hàng trăm trang xong mới thôi, cùng tinh thần "mở cổng ngay" ở trên).
  updateMarketCapCache();
  setInterval(updateMarketCapCache, MC_REFRESH_INTERVAL_MS);
}

start();
