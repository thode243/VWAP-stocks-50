/ 🔵 NIFTY50 Option Chain Updater (parallel, safer totals, expiry fallback)
function fetchOptionChainsAllNifty50() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // Set to a valid date (YYYY-MM-DD) to force an expiry, or leave null to let API choose nearest
  const FIXED_EXPIRY = null;

  // Symbols: deduped, corrected, lowercase slugs for the API
  const symbols = [
    "reliance","tcs","infy","hdfcbank","icicibank","sbilife","axisbank","lt","itc","sbin",
    "bhartiartl","kotakbank","hcltech","ongc","ntpc","techm","asianpaint","maruti","titan","sunpharma",
    "hindunilvr","ultracemco","powergrid","nestleind","cipla","tatamotors","tatasteel","coalindia",
    "drreddy","jswsteel","grasim","indusindbk","bajajfinsv","hdfclife","tataconsumer",
    "apollohosp","britannia","adaniports","bpcl","divislab","heromotoco","eichermot","upl",
    "hindalco","shriramfinance","zomato"
  ];

  const headers = {
    "Accept": "application/json",
    "Referer": "https://www.niftytrader.in/",
    "Origin": "https://www.niftytrader.in",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
  };

  const buildUrl = (sym, expiry) => {
    const base = "https://webapi.niftytrader.in/webapi/option/option-chain-data";
    const p = [
      `symbol=${encodeURIComponent(sym)}`,
      "exchange=nse",
      "atmBelow=0",
      "atmAbove=0"
    ];
    if (expiry) p.push(`expiryDate=${encodeURIComponent(expiry)}`);
    return `${base}?${p.join("&")}`;
  };

  const requests = symbols.map((sym) => ({
    url: buildUrl(sym.toLowerCase(), FIXED_EXPIRY),
    method: "get",
    headers,
    muteHttpExceptions: true,
    followRedirects: true
  }));

  // Fetch in parallel
  let responses;
  try {
    responses = UrlFetchApp.fetchAll(requests);
  } catch (e) {
    Logger.log(`❌ Parallel fetch failed: ${e.message}`);
    return;
  }

  responses.forEach((res, i) => {
    const symbol = symbols[i];
    try {
      let status = res.getResponseCode();
      let text = res.getContentText();
      let json = status === 200 ? safeParseJson(text) : null;

      let data = json?.resultData?.opDatas;
      // Fallback: try again without fixed expiry if empty or non-200
      if ((status !== 200) || !Array.isArray(data) || data.length === 0) {
        const retryRes = UrlFetchApp.fetch(buildUrl(symbol.toLowerCase(), null), {
          method: "get",
          headers,
          muteHttpExceptions: true,
          followRedirects: true
        });
        status = retryRes.getResponseCode();
        text = retryRes.getContentText();
        json = status === 200 ? safeParseJson(text) : null;
        data = json?.resultData?.opDatas;
      }

      if (status !== 200) {
        Logger.log(`⚠️ HTTP ${status} for ${symbol}`);
        return;
      }
      if (!Array.isArray(data) || data.length === 0) {
        Logger.log(`⚠️ No valid data for ${symbol}`);
        return;
      }

      const sheetName = `Option_${symbol.toUpperCase()}`;
      let sheet = ss.getSheetByName(sheetName);
      if (!sheet) {
        sheet = ss.insertSheet(sheetName);
      } else {
        sheet.clearContents();
      }

      const header = [
        "Strike",
        "Call OI","Call LTP","Call IV","Call VWAP","Call LTP - VWAP",
        "Put OI","Put LTP","Put IV","Put VWAP","Put LTP - VWAP",
        "Call Intrinsic","Put Intrinsic","Abs Diff (Call-Put)","Call + Put Diff","Spot",
        "Diff Amount (Q)","OI Diff (R)","R * Call VWAP (S)","R * Put VWAP (T)"
      ];
      const output = [header];

      let callDiffSum = 0;
      let putDiffSum = 0;

      data.forEach((item) => {
        const callLTP = num(item.calls_ltp);
        const callVWAP = num(item.calls_average_price);
        const putLTP = num(item.puts_ltp);
        const putVWAP = num(item.puts_average_price);

        const callDiff = callLTP - callVWAP;
        const putDiff = putLTP - putVWAP;

        callDiffSum += callDiff;
        putDiffSum += putDiff;

        output.push([
          num(item.strike_price),
          num(item.calls_oi), callLTP, num(item.calls_iv), callVWAP, callDiff,
          num(item.puts_oi),  putLTP,  num(item.puts_iv),  putVWAP,  putDiff,
          "", "", "", "", "", "", "", "", ""
        ]);
      });

      // Bulk write base data
      sheet.getRange(1, 1, output.length, header.length).setValues(output);

      // Spot: take from API when available; fallback to 0
      const spot = num(
        json?.resultData?.spotPrice ??
        json?.resultData?.underlyingValue ??
        json?.resultData?.ltp ??
        0
      );

      const lastRow = sheet.getLastRow();
      const dataRows = Math.max(0, lastRow - 1);
      if (dataRows === 0) {
        Logger.log(`⚠️ No data rows after write for ${symbol}`);
        return;
      }

      // Compute derived columns for rows 2..lastRow
      const base = sheet.getRange(2, 1, dataRows, 11).getValues();
      const calculated = base.map((row) => {
        const strike = num(row[0]);
        const callOI = num(row[1]);
        const callLTP = num(row[2]);
        const callVWAP = num(row[4]);
        const callDiff = num(row[5]);
        const putOI = num(row[6]);
        const putLTP = num(row[7]);
        const putVWAP = num(row[9]);
        const putDiff = num(row[10]);

        const callIntrinsic = Math.max(spot - strike, 0);
        const putIntrinsic = Math.max(strike - spot, 0);
        const absDiff = Math.abs(callDiff - putDiff);
        const sumDiff = callDiff + putDiff;

        const q = ((callOI * callLTP) - (putOI * putLTP)) / 10_000_000;
        const r = (callOI - putOI) / 1_000_000;
        const s = r * (-callVWAP);
        const t = r * putVWAP;

        return [callIntrinsic, putIntrinsic, absDiff, sumDiff, spot, q, r, s, t];
      });

      sheet.getRange(2, 12, calculated.length, 9).setValues(calculated);

      // Totals placed safely below the table, not inside it
      const summaryRow = output.length + 1;
      sheet.getRange(summaryRow, 5).setValue("Totals");
      sheet.getRange(summaryRow, 6).setValue(callDiffSum);
      sheet.getRange(summaryRow, 11).setValue(putDiffSum);

      // Minor UX: bold header, freeze
      sheet.getRange(1, 1, 1, header.length).setFontWeight("bold");
      sheet.setFrozenRows(1);

      Logger.log(`✅ ${sheetName} updated successfully.`);
    } catch (e) {
      Logger.log(`❌ Error processing ${symbol}: ${e.message}`);
    }
  });

  Logger.log("🏁 All NIFTY50 updates completed.");

  // Helpers
  function safeParseJson(text) {
    try { return JSON.parse(text); } catch (_e) { return null; }
  }
  function num(x) {
    const n = Number(x);
    return Number.isFinite(n) ? n : 0;
  }
}
