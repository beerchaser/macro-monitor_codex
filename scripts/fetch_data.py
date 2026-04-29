
#!/usr/bin/env python3
"""
macro-monitor 자동 업데이트 스크립트 v14
개선:
  - Brent(DCOILBRENTEU), WTI(DCOILWTICO) FRED 자동확인 추가
개선:
  - http_get: retry 3회 + exponential backoff
  - patch_html: 지표별 함수 분리
  - FRED API key 없으면 해당 항목 스킵 (전체 중단 없음)
"""

import urllib.request
import urllib.parse
import urllib.error
import json
import re
import os
import time
from datetime import datetime

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
MONITOR_FILE = "monitor.html"
AUTO_BADGE = '<span class="vbadge vbadge-auto">자동확인</span>'


# ── 공통 유틸 ────────────────────────────────────────────────────

def http_get(url, retries=3):
    """HTTP GET with retry (exponential backoff)"""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json"
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise


def http_get_raw(url, retries=3, encoding="utf-8"):
    """HTTP GET raw text (CSV 등)"""
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "*/*"
            })
            with urllib.request.urlopen(req, timeout=20) as resp:
                return resp.read().decode(encoding)
        except Exception:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise


def safe_fetch(name, fn):
    """실패해도 None 반환 — 이전 값 유지"""
    try:
        result = fn()
        print(f"  ✅ {name}: {result.get('display', str(result)[:30])}")
        return result
    except Exception as e:
        print(f"  ⚠️  {name}: 실패 ({e}) — 이전 값 유지")
        return None


def sub(html, pattern, replacement, flags=0, label=""):
    """regex 교체 + 결과 로깅"""
    result, n = re.subn(pattern, replacement, html, flags=flags)
    tag = label or pattern[:45]
    if n == 0:
        print(f"    ⚠️  미매칭: {tag}")
    else:
        print(f"    ✅ {n}건: {tag}")
    return result


# ── 데이터 조회 ──────────────────────────────────────────────────

def fetch_tga():
    data = http_get(
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
        "/v1/accounting/dts/operating_cash_balance"
        "?fields=record_date,account_type,open_today_bal"
        "&sort=-record_date&page[size]=20"
    )
    for row in data.get("data", []):
        if "Closing Balance" in row.get("account_type", ""):
            bal_b = float(row["open_today_bal"]) / 1_000
            dt = datetime.strptime(row["record_date"], "%Y-%m-%d")
            d = f"{dt.month}/{dt.day}"
            return {"bal_b": bal_b, "val_str": f"${bal_b:,.1f}B", "date": d,
                    "display": f"${bal_b:,.1f}B ({d})"}
    raise ValueError("TGA Closing Balance 없음")


def fetch_auction(term="10-Year", sec_type="Note"):
    url = (
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
        "/v1/accounting/od/auctions_query"
        f"?fields=auction_date,indirect_bidder_accepted,comp_accepted"
        f"&filter=security_type:eq:{sec_type},security_term:eq:{term}"
        "&sort=-auction_date&page[size]=1"
    )
    data = http_get(url)
    rows = data.get("data", [])
    if not rows:
        raise ValueError(f"{term} 경매 없음")
    r = rows[0]
    indirect = float(r["indirect_bidder_accepted"])
    comp = float(r["comp_accepted"])
    ratio = round(indirect / comp * 100, 1) if comp else 0
    dt = datetime.strptime(r["auction_date"], "%Y-%m-%d")
    d = f"{dt.month}/{dt.day}"
    return {"ratio": ratio, "date": d, "display": f"{ratio}% IB ({d})"}


def fetch_fred(series_id):
    if not FRED_API_KEY:
        raise ValueError("FRED_API_KEY 없음")
    params = urllib.parse.urlencode({
        "series_id": series_id, "api_key": FRED_API_KEY,
        "file_type": "json", "sort_order": "desc", "limit": 5
    })
    data = http_get(f"https://api.stlouisfed.org/fred/series/observations?{params}")
    for obs in data.get("observations", []):
        if obs["value"] != ".":
            val = float(obs["value"])
            dt = datetime.strptime(obs["date"], "%Y-%m-%d")
            d = f"{dt.month}/{dt.day}"
            return {"val": val, "date": d, "display": f"{val} ({d})"}
    raise ValueError(f"{series_id} 없음")


def fetch_nfp():
    """NFP 전월 대비 증감 (천명)"""
    if not FRED_API_KEY:
        raise ValueError("FRED_API_KEY 없음")
    params = urllib.parse.urlencode({
        "series_id": "PAYEMS", "api_key": FRED_API_KEY,
        "file_type": "json", "sort_order": "desc", "limit": 3
    })
    data = http_get(f"https://api.stlouisfed.org/fred/series/observations?{params}")
    obs = [o for o in data.get("observations", []) if o["value"] != "."]
    if len(obs) < 2:
        raise ValueError("NFP 데이터 부족")
    latest = float(obs[0]["value"])
    prev = float(obs[1]["value"])
    change = round(latest - prev)
    dt = datetime.strptime(obs[0]["date"], "%Y-%m-%d")
    month_name = f"{dt.month}월"
    return {"val": change, "month": month_name, "date": f"{dt.month}/{dt.day}",
            "display": f"+{change:,}K ({month_name})"}


def fetch_cot_ust10y():
    """CFTC TFF — 10Y UST 레버리지드 펀드 Net 포지션 (계약코드 043602)"""
    import io, csv
    params = urllib.parse.urlencode({
        "$where": "cftc_contract_market_code='043602'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": "1",
        "$select": "report_date_as_yyyy_mm_dd,lev_money_positions_long,lev_money_positions_short"
    })
    url = f"https://publicreporting.cftc.gov/resource/udgc-27he.csv?{params}"
    raw = http_get_raw(url)
    reader = csv.DictReader(io.StringIO(raw))
    for row in reader:
        long_pos = int(float(row.get("lev_money_positions_long", 0) or 0))
        short_pos = int(float(row.get("lev_money_positions_short", 0) or 0))
        net = long_pos - short_pos
        dt = datetime.strptime(row["report_date_as_yyyy_mm_dd"][:10], "%Y-%m-%d")
        d = f"{dt.month}/{dt.day}"
        direction = "Net Short" if net < 0 else "Net Long"
        contracts_k = abs(net) // 1000
        return {"net": net, "date": d, "direction": direction,
                "contracts_k": contracts_k,
                "display": f"{direction} {contracts_k}K계약 ({d})"}
    raise ValueError("043602 행 없음")


def fetch_oil(series_id):
    """Brent(DCOILBRENTEU) / WTI(DCOILWTICO) — FRED, 전일 종가"""
    return fetch_fred(series_id)


# ── CSS 보장 ────────────────────────────────────────────────────


def fetch_reserves():
    """은행 지준 잔고 — FRED WRBWFRBL (H.4.1 주간, 단위: 백만달러)"""
    r = fetch_fred("WRBWFRBL")
    r["val_b"] = r["val"] / 1_000  # M → B
    r["display"] = f'${r["val_b"]:,.0f}B ({r["date"]})'
    return r


def fetch_walcl():
    """Fed 대차대조표 총자산 — FRED WALCL (H.4.1 주간, 단위: 백만달러)"""
    r = fetch_fred("WALCL")
    r["val_b"] = r["val"] / 1_000
    r["display"] = f'${r["val_b"]:,.0f}B ({r["date"]})'
    return r


def fetch_deposits():
    """은행 예금 총액 — FRED DPSACBW027SBOG (H.8 주간, 단위: 십억달러)"""
    r = fetch_fred("DPSACBW027SBOG")
    r["display"] = f'${r["val"]:,.0f}B ({r["date"]})'
    return r



def fetch_usdjpy():
    """USD/JPY 환율 — FRED DEXJPUS (일간)"""
    return fetch_fred("DEXJPUS")


def fetch_stlfsi():
    """St. Louis Fed 금융 스트레스 지수 — FRED STLFSI4 (주간)"""
    return fetch_fred("STLFSI4")

def fetch_fhlb():
    """FHLB Advances — FRED BOGZ1FL403069330Q (Fed Financial Accounts Z.1, 분기)
    FHLB 공식 보고서($676.7B)와 ~$10B 차이 — 집계 방식 상이, 추세 추적용으로 동일하게 유효
    공식 수치: https://www.fhlb-of.com/ofweb_userWeb/pageBuilder/fhlbank-combined-financial-report
    """
    r = fetch_fred("BOGZ1FL403069330Q")
    r["val_b"] = r["val"] / 1_000
    r["display"] = f'${r["val_b"]:,.1f}B ({r["date"]})'
    return r

def fetch_oas(series_id):
    """IG OAS(BAMLC0A0CM) / HY OAS(BAMLH0A0HYM2) — FRED, 단위: %"""
    return fetch_fred(series_id)


def ensure_css(html):
    if 'vbadge-auto' not in html:
        old = '.vbadge-ss{background:#E6F1FB;color:#0C447C}'
        new = old + '\n.vbadge-auto{background:#EDE7F6;color:#4527A0}'
        html = html.replace(old, new)
        print("  [CSS] vbadge-auto 삽입됨")
    return html


# ── 지표별 패치 함수 ─────────────────────────────────────────────

def patch_tga(html, tga):
    if not tga:
        return html
    # val+note 전체 블록: DTS Closing note 텍스트로 정확히 타겟팅
    note_pat = re.compile(r'\d+/\d+ DTS Closing \$[\d,.]+B · fiscaldata\.treasury\.gov')
    m_note = note_pat.search(html)
    if not m_note:
        print(f"    ⚠️  미매칭: TGA")
        return html
    new_note = f'{tga["date"]} DTS Closing {tga["val_str"]} · fiscaldata.treasury.gov'
    html = html[:m_note.start()] + new_note + html[m_note.end():]
    # val 교체: 새 note 앞 150자 안에서
    pos = html.find(new_note)
    segment = html[max(0, pos-150):pos]
    new_segment = re.sub(
        r'(>\$)[\d,.]+B(</td>)',
        lambda x: f'{x.group(1)}{tga["bal_b"]:,.1f}B{x.group(2)}',
        segment, count=1
    )
    html = html[:max(0, pos-150)] + new_segment + html[pos:]
    print(f"    ✅ TGA {tga['val_str']} ({tga['date']})")

    html = sub(html,
        r'\d+/\d+ DTS Closing \$[\d,.]+B · fiscaldata\.treasury\.gov',
        f'{tga["date"]} DTS Closing {tga["val_str"]} · fiscaldata.treasury.gov',
        label="TGA note")
    html = sub(html,
        r'\d+/\d+ DTS Closing \$[\d,.]+B\(전일',
        f'{tga["date"]} DTS Closing {tga["val_str"]}(전일',
        label="TGA threshold")
    return html


def patch_rrp(html, rrp):
    if not rrp:
        return html
    html = sub(html,
        r'(<td class="val val-ok">\$)[\d.]+B(</td>\s*<td class="verify">.*?RRPONTSYD)',
        lambda m: f'{m.group(1)}{rrp["val"]:.2f}B{m.group(2)}',
        re.DOTALL, "RRP val")
    html = sub(html,
        r'\d+/\d+ · FRED RRPONTSYD [\d.]+B',
        f'{rrp["date"]} · FRED RRPONTSYD {rrp["val"]:.3f}B',
        label="RRP note")
    return html


def patch_dgs10(html, dgs10):
    if not dgs10:
        return html
    html = sub(html,
        r'(<td class="val val-ok">)([\d.]+%)(</td>\s*<td class="verify">.*?FRED DGS10)',
        lambda m: f'{m.group(1)}{dgs10["val"]:.2f}%{m.group(3)}',
        re.DOTALL, "DGS10 val")
    html = sub(html,
        r'\d+/\d+ 종가 · FRED DGS10',
        f'{dgs10["date"]} 종가 · FRED DGS10',
        label="DGS10 note")
    return html


def patch_sofr(html, sofr):
    if not sofr:
        return html
    html = sub(html,
        r'(<td class="val val-ok">)([\d.]+%)(\ / [\d.]+%</td>)',
        lambda m: f'{m.group(1)}{sofr["val"]:.2f}%{m.group(3)}',
        label="SOFR val")
    html = sub(html,
        r'\d+/\d+ SOFR [\d.]+% · FRED 확인',
        f'{sofr["date"]} SOFR {sofr["val"]:.2f}% · FRED 확인',
        label="SOFR note")
    html = sub(html,
        r'\d+/\d+ SOFR [\d.]+% vs IORB [\d.]+% · 역전 해소',
        f'{sofr["date"]} SOFR {sofr["val"]:.2f}% vs IORB 3.65% · 역전 해소',
        label="Repo Stress note")
    return html


def patch_auction(html, auction):
    if not auction:
        return html
    html = sub(html,
        r'(<td class="val val-(?:ok|warn)">)[\d.]+% \((?:30Y|10Y)\)(</td>)',
        f'\\g<1>{auction["ratio"]}% (10Y)\\g<2>',
        label="경매 val")
    html = sub(html,
        r'\d+Y \d+/\d+ 경매 · fiscaldata 확인',
        f'10Y {auction["date"]} 경매 · fiscaldata 확인',
        label="경매 note")
    return html


def patch_nfp(html, nfp):
    if not nfp:
        return html
    nfp_k = nfp["val"]
    month = nfp["month"]
    html = sub(html,
        r'(<td class="val val-(?:ok|warn)">)\d+월: [+-][\d,]+K(</td>)',
        f'\\g<1>{month}: +{nfp_k:,}K\\g<2>',
        label="NFP val")
    html = sub(html,
        r'\d+/\d+ 발표 · BLS \d+월 [+-][\d,]+K',
        f'{nfp["date"]} 발표 · BLS {month} +{nfp_k:,}K',
        label="NFP note")
    return html


def patch_cpi(html, cpi):
    if not cpi:
        return html
    html = sub(html,
        r'\d+/\d+ 발표 · 3월 BLS 헤드라인',
        f'{cpi["date"]} 발표 · 3월 BLS 헤드라인',
        label="CPI note")
    return html


def patch_unrate(html, unrate):
    if not unrate:
        return html
    html = sub(html,
        r'(<td class="val val-(?:ok|warn)">)([\d.]+%)(</td>\s*<td class="verify">.*?3월 BLS)',
        lambda m: f'{m.group(1)}{unrate["val"]:.1f}%{m.group(3)}',
        re.DOTALL, "실업률 val")
    html = sub(html,
        r'3월 BLS · \d+/\d+ 발표',
        f'3월 BLS · {unrate["date"]} 발표',
        label="실업률 note")
    return html


def patch_ci(html, ci):
    if not ci:
        return html
    old_badge = (
        '<span class="vbadge vbadge-old">구버전</span>'
        '<span class="verify-note">3월 FRED BUSLOANS · 접근 불가</span>'
    )
    new_badge = f'{AUTO_BADGE}<span class="verify-note">{ci["date"]} · FRED BUSLOANS</span>'
    if old_badge in html:
        html = html.replace(old_badge, new_badge)
        print(f"    ✅ C&I badge+note")
    else:
        html = sub(html,
            r'(<td class="verify">)<span class="vbadge [^"]+">[^<]+</span>'
            r'(<span class="verify-note">)\d+/\d+ · FRED BUSLOANS',
            f'\\g<1>{AUTO_BADGE}\\g<2>{ci["date"]} · FRED BUSLOANS',
            label="C&I note update")
    return html


def patch_spx(html, spx):
    if not spx:
        return html
    # val: <br> 포함 패턴
    html = sub(html,
        r'(<td class="val val-(?:ok|warn)">)([\d,]+\.?\d*)(<br>(?:<br>)?</td>\s*<td class="verify">.*?SP500)',
        lambda m: f'{m.group(1)}{spx["val"]:,.2f}{m.group(3)}',
        re.DOTALL, "SPX val")
    html = sub(html,
        r'\d+/\d+ 종가 · FRED SP500',
        f'{spx["date"]} 종가 · FRED SP500',
        label="SPX note")
    return html


def patch_vix(html, vix):
    if not vix:
        return html
    # val+note 전체 블록을 note anchor로 정확히 교체
    m = re.search(
        r'<td class="val val-(?:ok|warn)">[^<]+</td>\s*'
        r'<td class="verify"><span[^>]*>[^<]*</span>'
        r'<span class="verify-note">\d+/\d+ 종가 · FRED VIXCLS</span></td>',
        html
    )
    if not m:
        print(f"    ⚠️  미매칭: VIX")
        return html
    old_str = m.group(0)
    status = "val-warn" if vix["val"] >= 20 else "val-ok"
    new_str = (
        f'<td class="val {status}">{vix["val"]:.2f}</td>\n  '
        f'<td class="verify"><span class="vbadge vbadge-auto">자동확인</span>'
        f'<span class="verify-note">{vix["date"]} 종가 · FRED VIXCLS</span></td>'
    )
    html = html.replace(old_str, new_str, 1)
    print(f"    ✅ VIX {vix['val']:.2f} ({vix['date']})")
    return html


def patch_dxy(html, dxy):
    if not dxy:
        return html
    # 직접 교체 (초기) or 재업데이트
    old_ok = re.search(
        r'<td class="val val-(?:ok|warn)">[\d.]+</td>\s*'
        r'<td class="verify"><span class="vbadge [^"]+">[^<]+</span>'
        r'<span class="verify-note">\d+/\d+ 종가 · (?:Investing\.com|FRED DTWEXBGS)</span></td>',
        html
    )
    if old_ok:
        old_str = old_ok.group(0)
        new_str = re.sub(r'(>)[\d.]+(<\/td>)', f'\\g<1>{dxy["val"]:.2f}\\g<2>', old_str, count=1)
        new_str = re.sub(r'\d+/\d+ 종가 · (?:Investing\.com|FRED DTWEXBGS)',
                         f'{dxy["date"]} 종가 · FRED DTWEXBGS', new_str)
        new_str = new_str.replace('vbadge-ok">검색확인', 'vbadge-auto">자동확인')
        html = html.replace(old_str, new_str, 1)
        print(f"    ✅ DXY val+note")
    else:
        print(f"    ⚠️  미매칭: DXY")
    return html


def patch_cot(html, cot):
    if not cot:
        return html
    net_k = abs(cot["net"]) // 1000
    direction = cot["direction"]

    # COT 국채 val
    html = sub(html,
        r'(<td class="val val-(?:ok|warn)">)Net (?:Short|Long) [\d,]+K계약(</td>)',
        f'\\g<1>{direction} {net_k:,}K계약\\g<2>',
        label="COT val (국채)")
    html = sub(html,
        r'\d+/\d+ · CFTC TFF Lev Funds(?=</span></td>.*?구버전|</span></td>)',
        f'{cot["date"]} · CFTC TFF Lev Funds',
        label="COT note (국채)")

    # HF숏 val
    html = sub(html,
        r'(<td class="val val-(?:ok|warn)">~?)[\d,]+K계약(</td>)',
        f'\\g<1>{net_k:,}K계약\\g<2>',
        label="COT val (HF숏)")

    # 두 군데 배지 모두 자동확인으로
    html = sub(html,
        r'(<td class="verify">)<span class="vbadge [^"]+">[^<]+</span>'
        r'(<span class="verify-note">)\d+/\d+ · CFTC TFF Lev Funds',
        f'\\g<1>{AUTO_BADGE}\\g<2>{cot["date"]} · CFTC TFF Lev Funds',
        label="COT badge+note")
    return html


def patch_brent(html, brent):
    if not brent:
        return html
    html = sub(html,
        r'(<td class="val val-(?:ok|warn)">)\$?[\d.]+(\s*(?:/bbl)?</td>\s*<td class="verify">.*?DCOILBRENTEU)',
        lambda m: f'{m.group(1)}${brent["val"]:.1f}{m.group(2)}',
        re.DOTALL, "Brent val")
    html = sub(html,
        r'\d+/\d+ 종가 · FRED DCOILBRENTEU',
        f'{brent["date"]} 종가 · FRED DCOILBRENTEU',
        label="Brent note")
    # 배지 자동확인으로 업그레이드
    html = sub(html,
        r'(<td class="verify">)<span class="vbadge [^"]+">[^<]+</span>'
        r'(<span class="verify-note">\d+/\d+ 종가 · FRED DCOILBRENTEU)',
        f'\\g<1>{AUTO_BADGE}\\g<2>',
        label="Brent badge")
    return html


def patch_wti(html, wti):
    if not wti:
        return html
    html = sub(html,
        r'(<td class="val val-(?:ok|warn)">)\$?[\d.]+(\s*(?:/bbl)?</td>\s*<td class="verify">.*?DCOILWTICO)',
        lambda m: f'{m.group(1)}${wti["val"]:.1f}{m.group(2)}',
        re.DOTALL, "WTI val")
    html = sub(html,
        r'\d+/\d+ 종가 · FRED DCOILWTICO',
        f'{wti["date"]} 종가 · FRED DCOILWTICO',
        label="WTI note")
    html = sub(html,
        r'(<td class="verify">)<span class="vbadge [^"]+">[^<]+</span>'
        r'(<span class="verify-note">\d+/\d+ 종가 · FRED DCOILWTICO)',
        f'\\g<1>{AUTO_BADGE}\\g<2>',
        label="WTI badge")
    return html


# ── 메인 패치 ────────────────────────────────────────────────────

def patch_ig_oas(html, ig):
    if not ig:
        return html
    bp = round(ig["val"] * 100)
    # note 텍스트로 먼저 찾아서 교체 — regex 충돌 방지
    note_pat = re.compile(r'FRED \d+/\d+ · [\d.]+% · BAMLC0A0CM')
    m_note = note_pat.search(html)
    if not m_note:
        print(f"    ⚠️  미매칭: IG OAS")
        return html
    new_note = f'FRED {ig["date"]} · {ig["val"]:.2f}% · BAMLC0A0CM'
    html = html[:m_note.start()] + new_note + html[m_note.end():]
    # val 교체: 새 note 앞에 있는 bp 값
    pos = html.find(new_note)
    segment = html[max(0, pos-150):pos]
    new_segment = re.sub(r'(>)\d+bp(</td>\s*$)', lambda x: f'{x.group(1)}{bp}bp{x.group(2)}', segment, count=1, flags=re.MULTILINE)
    html = html[:max(0, pos-150)] + new_segment + html[pos:]
    print(f"    ✅ IG OAS {bp}bp ({ig['date']})")
    return html

def patch_hy_oas(html, hy):
    if not hy:
        return html
    bp = round(hy["val"] * 100)
    note_pat = re.compile(r'FRED \d+/\d+ · [\d.]+% · BAMLH0A0HYM2')
    m_note = note_pat.search(html)
    if not m_note:
        print(f"    ⚠️  미매칭: HY OAS")
        return html
    new_note = f'FRED {hy["date"]} · {hy["val"]:.2f}% · BAMLH0A0HYM2'
    html = html[:m_note.start()] + new_note + html[m_note.end():]
    pos = html.find(new_note)
    segment = html[max(0, pos-150):pos]
    new_segment = re.sub(r'(>)\d+bp(</td>\s*$)', lambda x: f'{x.group(1)}{bp}bp{x.group(2)}', segment, count=1, flags=re.MULTILINE)
    html = html[:max(0, pos-150)] + new_segment + html[pos:]
    print(f"    ✅ HY OAS {bp}bp ({hy['date']})")
    return html

def patch_reserves(html, res):
    if not res:
        return html
    val_b = res["val"] / 1_000
    status = "val-warn" if val_b < 3000 else "val-ok"
    # val+note 직접 교체
    html = re.sub(
        r'(<td class="val val-(?:ok|warn)">)\$[\d,]+B(</td>\s*<td class="verify"><span[^>]*>[^<]*</span><span class="verify-note">\d+/\d+ · FRED WRBWFRBL</span></td>)',
        lambda m: f'<td class="val {status}">${val_b:,.0f}B' + m.group(2),
        html, count=1
    )
    html = sub(html,
        r'\d+/\d+ · FRED WRBWFRBL',
        f'{res["date"]} · FRED WRBWFRBL',
        label="지준 note")
    return html


def patch_walcl(html, walcl):
    if not walcl:
        return html
    val_b = walcl["val"] / 1_000
    html = re.sub(
        r'(<td class="val val-(?:ok|warn)">)\$[\d,]+B(</td>\s*<td class="verify"><span[^>]*>[^<]*</span><span class="verify-note">\d+/\d+ · FRED WALCL</span></td>)',
        lambda m: f'<td class="val val-ok">${val_b:,.0f}B' + m.group(2),
        html, count=1
    )
    html = sub(html,
        r'\d+/\d+ · FRED WALCL',
        f'{walcl["date"]} · FRED WALCL',
        label="WALCL note")
    return html


def patch_deposits(html, dep):
    if not dep:
        return html
    html = re.sub(
        r'(<td class="val val-(?:ok|warn)">)\$[\d,]+B(</td>\s*<td class="verify"><span[^>]*>[^<]*</span><span class="verify-note">\d+/\d+ · FRED DPSACBW027SBOG</span></td>)',
        lambda m: f'<td class="val val-ok">${dep["val"]:,.0f}B' + m.group(2),
        html, count=1
    )
    html = sub(html,
        r'\d+/\d+ · FRED DPSACBW027SBOG',
        f'{dep["date"]} · FRED DPSACBW027SBOG',
        label="예금 note")
    return html


def patch_usdjpy(html, usdjpy):
    if not usdjpy:
        return html
    m = re.search(
        r'<td class="val val-(?:ok|warn|alert)">[\d.]+</td>\s*'
        r'<td class="verify"><span[^>]*>[^<]*</span>'
        r'<span class="verify-note">\d+/\d+ 종가 · FRED DEXJPUS</span></td>',
        html
    )
    if m:
        old = m.group(0)
        new = re.sub(r'(>)[\d.]+(</td>\s*<td class="verify">)', lambda x: f'{x.group(1)}{usdjpy["val"]:.2f}{x.group(2)}', old, count=1)
        new = re.sub(r'\d+/\d+ 종가 · FRED DEXJPUS', f'{usdjpy["date"]} 종가 · FRED DEXJPUS', new)
        new = new.replace('vbadge-ok">검색확인', 'vbadge-auto">자동확인')
        html = html.replace(old, new, 1)
        print(f"    ✅ USD/JPY {usdjpy['val']:.2f} ({usdjpy['date']})")
    else:
        print(f"    ⚠️  미매칭: USD/JPY")
    return html


def patch_stlfsi(html, stlfsi):
    if not stlfsi:
        return html
    # note anchor로 정확히 1건만 교체
    m = re.search(
        r'<td class="val val-(?:ok|warn)">[^<]+</td>\s*'
        r'<td class="verify"><span[^>]*>[^<]*</span>'
        r'<span class="verify-note">\d+/\d+ · FRED STLFSI4</span></td>',
        html
    )
    if not m:
        print(f"    ⚠️  미매칭: STLFSI4")
        return html
    old_str = m.group(0)
    new_str = (
        f'<td class="val val-ok">{stlfsi["val"]:.3f}</td>\n  '
        f'<td class="verify"><span class="vbadge vbadge-auto">자동확인</span>'
        f'<span class="verify-note">{stlfsi["date"]} · FRED STLFSI4</span></td>'
    )
    html = html.replace(old_str, new_str, 1)
    print(f"    ✅ STLFSI4 {stlfsi['val']:.3f} ({stlfsi['date']})")
    return html


def patch_fhlb(html, fhlb):
    if not fhlb:
        return html
    val_b = fhlb["val"] / 1_000
    # val 교체: FHLB verify-note anchor 기반
    m = re.search(
        r'<td class="val val-(?:ok|warn)">[^<]+</td>\s*'
        r'<td class="verify"><span[^>]*>[^<]*</span>'
        r'<span class="verify-note">Q\d 20\d\d · FHLB',
        html
    )
    if m:
        old_str = m.group(0)
        new_str = re.sub(
            r'(<td class="val val-(?:ok|warn)">)[^<]+(</td>)',
            lambda x: f'{x.group(1)}${val_b:,.1f}B{x.group(2)}',
            old_str, count=1
        )
        new_str = re.sub(
            r'Q\d 20\d\d · FHLB[^<]*',
            f'Q4 {fhlb["date"][:4]} · FRED Z.1 (FHLB 공식≈$676.7B)',
            new_str
        )
        new_str = new_str.replace(
            'vbadge-ok">검색확인',
            'vbadge-auto">자동확인'
        )
        html = html.replace(old_str, new_str, 1)
        print(f"    ✅ FHLB ${val_b:,.1f}B ({fhlb['date']})")
    else:
        print(f"    ⚠️  미매칭: FHLB")
    return html


def validate_patches(html, data):
    """패치 후 날짜 기준으로 누락 의심 항목 체크"""
    missing = []
    skip = {"cpi", "ci"}
    for key, val in data.items():
        if not val or key in skip:
            continue
        date = val.get("date", "")
        if date and date not in html:
            missing.append(f"{key}({date})")
    if missing:
        print(f"  ⚠️  패치 후 날짜 미반영 의심: {', '.join(missing)}")
    else:
        print(f"  ✅ 패치 검증 OK")


def patch_html(html, data):
    print("\n  [패치 시작]")
    html = patch_tga(html,     data.get("tga"))
    html = patch_rrp(html,     data.get("rrp"))
    html = patch_dgs10(html,   data.get("dgs10"))
    html = patch_sofr(html,    data.get("sofr"))
    html = patch_auction(html, data.get("auction"))
    html = patch_nfp(html,     data.get("nfp"))
    html = patch_cpi(html,     data.get("cpi"))
    html = patch_unrate(html,  data.get("unrate"))
    html = patch_ci(html,      data.get("ci"))
    html = patch_spx(html,     data.get("spx"))
    html = patch_vix(html,     data.get("vix"))
    html = patch_dxy(html,     data.get("dxy"))
    html = patch_cot(html,     data.get("cot"))
    html = patch_brent(html,   data.get("brent"))
    html = patch_wti(html,     data.get("wti"))
    html = patch_ig_oas(html,  data.get("ig_oas"))
    html = patch_hy_oas(html,  data.get("hy_oas"))
    html = patch_reserves(html, data.get("reserves"))
    html = patch_walcl(html,    data.get("walcl"))
    html = patch_deposits(html,  data.get("deposits"))
    html = patch_fhlb(html,      data.get("fhlb"))
    html = patch_usdjpy(html,   data.get("usdjpy"))
    html = patch_stlfsi(html,   data.get("stlfsi"))
    return html


# ── Main ─────────────────────────────────────────────────────────

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] 데이터 조회 시작\n")

    data = {}
    data["tga"]     = safe_fetch("TGA",     fetch_tga)
    data["dgs10"]   = safe_fetch("DGS10",   lambda: fetch_fred("DGS10"))
    data["sofr"]    = safe_fetch("SOFR",    lambda: fetch_fred("SOFR"))
    data["rrp"]     = safe_fetch("RRP",     lambda: fetch_fred("RRPONTSYD"))
    data["nfp"]     = safe_fetch("NFP",     fetch_nfp)
    data["cpi"]     = safe_fetch("CoreCPI", lambda: fetch_fred("CPILFESL"))
    data["unrate"]  = safe_fetch("UNRATE",  lambda: fetch_fred("UNRATE"))
    data["ci"]      = safe_fetch("C&I",     lambda: fetch_fred("BUSLOANS"))
    data["auction"] = safe_fetch("경매IB",  fetch_auction)
    data["spx"]     = safe_fetch("S&P500",  lambda: fetch_fred("SP500"))
    data["vix"]     = safe_fetch("VIX",     lambda: fetch_fred("VIXCLS"))
    data["dxy"]     = safe_fetch("DXY",     lambda: fetch_fred("DTWEXBGS"))
    data["cot"]     = safe_fetch("COT_UST", fetch_cot_ust10y)
    data["brent"]   = safe_fetch("Brent",   lambda: fetch_oil("DCOILBRENTEU"))
    data["wti"]     = safe_fetch("WTI",     lambda: fetch_oil("DCOILWTICO"))
    data["ig_oas"]  = safe_fetch("IG OAS",  lambda: fetch_oas("BAMLC0A0CM"))
    data["hy_oas"]  = safe_fetch("HY OAS",  lambda: fetch_oas("BAMLH0A0HYM2"))
    data["reserves"] = safe_fetch("지준",     fetch_reserves)
    data["walcl"]    = safe_fetch("WALCL",    fetch_walcl)
    data["deposits"] = safe_fetch("예금",     fetch_deposits)
    data["fhlb"]     = safe_fetch("FHLB",     fetch_fhlb)
    data["usdjpy"]   = safe_fetch("USD/JPY",  fetch_usdjpy)
    data["stlfsi"]   = safe_fetch("STLFSI4",  fetch_stlfsi)

    with open(MONITOR_FILE, encoding="utf-8") as f:
        html = f.read()

    html = ensure_css(html)
    html = patch_html(html, data)
    validate_patches(html, data)

    with open(MONITOR_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n  완료: {MONITOR_FILE} 업데이트됨")


if __name__ == "__main__":
    main()
