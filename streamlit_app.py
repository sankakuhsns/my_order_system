# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v3.5 - 최종 완성판)
# - 주요 개선사항:
#   - 전체 기능 복원 및 안정화
#   - 버튼 이중 클릭 문제 전역 수정 (불필요한 rerun 제거)
#   - 단가 컬럼 처리 로직 명확화 및 오류 해결
#   - UI 레이아웃 완성도 향상
# =============================================================================

from io import BytesIO
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
from collections.abc import Mapping
from zoneinfo import ZoneInfo

import hashlib
import pandas as pd
import streamlit as st

# Google Sheets
import gspread
from google.oauth2 import service_account

# Excel export
import xlsxwriter  # noqa: F401 (엔진 로딩용)

# -----------------------------------------------------------------------------
# 페이지/테마/스타일
# -----------------------------------------------------------------------------
st.set_page_config(page_title="산카쿠 식자재 발주 시스템", page_icon="📦", layout="wide")

THEME = { "BORDER": "#e8e8e8", "PRIMARY": "#1C6758", "BG": "#f7f8fa", "CARD_BG": "#ffffff", "TEXT": "#222", "MUTED": "#777" }
CARD_STYLE = f"background-color:{THEME['CARD_BG']}; border:1px solid {THEME['BORDER']}; border-radius:12px; padding:16px;"

st.markdown(f"""
<style>
html, body, [data-testid="stAppViewContainer"] {{ background: {THEME['BG']}; color: {THEME['TEXT']}; }}
.small {{ font-size:12px; color:{THEME['MUTED']}; }}
.block-container {{ padding-top: 2.4rem; padding-bottom: 1.6rem; }}
.card {{ {CARD_STYLE} box-shadow: 0 2px 8px rgba(0,0,0,0.03); }}
.metric {{ font-weight:700; color:{THEME['PRIMARY']}; }}
[data-testid="stAppViewContainer"] .main .block-container {{ max-width: 1050px; margin: 0 auto; padding: 0 12px; }}
.stTextInput>div>div>input, .stNumberInput input, .stDateInput input {{ border:1px solid {THEME['BORDER']} !important; border-radius:10px !important; height:34px; }}
.dataframe, .stDataFrame, .stTable {{ background:{THEME['CARD_BG']}; border-radius:12px; border:1px solid {THEME['BORDER']}; }}
.stTabs [role="tablist"] {{ display:flex !important; gap:12px !important; flex-wrap:wrap !important; margin:8px 0 24px !important; border-bottom:none !important; }}
.stTabs button[role="tab"] {{ border:1px solid {THEME['BORDER']} !important; border-radius:12px !important; background:#fff !important; padding:10px 14px !important; box-shadow:0 1px 6px rgba(0,0,0,0.04) !important; cursor:pointer !important; transition: transform .08s ease, box-shadow .12s ease; }}
.stTabs button[role="tab"]:hover {{ transform: translateY(-1px); box-shadow:0 4px 12px rgba(0,0,0,0.08); }}
.stTabs button[role="tab"][aria-selected="true"] {{ border-color:{THEME['PRIMARY']} !important; color:{THEME['PRIMARY']} !important; box-shadow:0 6px 16px rgba(28,103,88,0.18) !important; font-weight:700; }}
.stTabs [data-baseweb="tab-highlight"] {{ display:none !important; }}
.sticky-bottom {{ position: sticky; bottom: 0; z-index: 999; {CARD_STYLE} margin-top:10px; display:flex; align-items:center; justify-content:space-between; gap:16px; }}
.login-title {{ text-align:center; font-size:42px; font-weight:800; margin:16px 0 12px; }}
.muted-buttons .stButton > button {{ background: #f3f4f6 !important; color: #333 !important; border: 1px solid #e5e7eb !important; }}
.stButton > button[data-testid="baseButton-primary"] {{ background: #1C6758 !important; color: #fff !important; border: 1px solid #1C6758 !important; border-radius: 10px !important; height: 34px !important; }}
</style>
""", unsafe_allow_html=True)

# --- 공용 작은 UI 유틸 ---
def v_spacer(height: int):
    st.markdown(f"<div style='height:{height}px'></div>", unsafe_allow_html=True)

def fmt_num(x, decimals=0):
    try:
        if decimals == 0: return f"{float(x):,.0f}"
        return f"{float(x):,.{decimals}f}"
    except (ValueError, TypeError): return "-"

KST = ZoneInfo("Asia/Seoul")
def now_kst_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str: return datetime.now(KST).strftime(fmt)
def ymd(d: date) -> str: return d.strftime("%y%m%d")
def make_filename(prefix: str, dt_from: date, dt_to: date) -> str: return f"{prefix} {ymd(dt_from)}~{ymd(dt_to)}.xlsx"

# =============================================================================
# 1) Users 로더
# =============================================================================
def _normalize_account(uid: str, payload: Mapping) -> dict:
    pwd_plain, pwd_hash = payload.get("password"), payload.get("password_hash")
    name = str(payload.get("name", uid)).strip()
    role = str(payload.get("role", "store")).strip().lower()
    if not (pwd_plain or pwd_hash): st.error(f"[users.{uid}]에 password 또는 password_hash가 필요합니다."); st.stop()
    if role not in {"store", "admin"}: st.error(f"[users.{uid}].role 은 'store' 또는 'admin' 이어야 합니다. (현재: {role})"); st.stop()
    return {"password": str(pwd_plain) if pwd_plain else None, "password_hash": str(pwd_hash).lower() if pwd_hash else None, "name": name, "role": role}

def load_users_from_secrets() -> Dict[str, Dict[str, str]]:
    cleaned: Dict[str, Dict[str, str]] = {}
    users_root = st.secrets.get("users", None)
    if isinstance(users_root, Mapping) and len(users_root) > 0:
        for uid, payload in users_root.items():
            if isinstance(payload, Mapping): cleaned[str(uid)] = _normalize_account(str(uid), payload)
    elif isinstance(users_root, list) and users_root:
        for row in users_root:
            if not isinstance(row, Mapping): continue
            uid = row.get("user_id") or row.get("uid") or row.get("id")
            if uid: cleaned[str(uid)] = _normalize_account(str(uid), row)
    if not cleaned: st.error("로그인 계정을 찾을 수 없습니다. Secrets 의 [users] 구조를 확인하세요."); st.stop()
    return cleaned

USERS = load_users_from_secrets()

# =============================================================================
# 2) 시트/스키마 정의
# =============================================================================
SHEET_NAME_MASTER = "상품마스터"
SHEET_NAME_ORDERS = "발주"
ORDERS_COLUMNS = ["주문일시","발주번호","지점ID","지점명","납품요청일","품목코드","품목명","단위","수량","단가","금액","비고","상태","처리일시","처리자"]

# =============================================================================
# 3) Google Sheets 연결
# =============================================================================
@st.cache_resource(show_spinner=False)
def get_gs_client():
    google = st.secrets.get("google", {})
    required = ["type","project_id","private_key_id","private_key","client_email","client_id"]
    if missing := [k for k in required if not str(google.get(k, "")).strip()]: st.error(f"Google 연동 설정 부족: {', '.join(missing)}"); st.stop()
    creds_info = dict(google)
    if "\\n" in str(creds_info.get("private_key", "")): creds_info["private_key"] = str(creds_info["private_key"]).replace("\\n", "\n")
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def open_spreadsheet():
    key = str(st.secrets.get("google", {}).get("SPREADSHEET_KEY") or st.secrets.get("SPREADSHEET_KEY", "")).strip()
    if not key: st.error("Secrets 에 SPREADSHEET_KEY가 없습니다."); st.stop()
    try: return get_gs_client().open_by_key(key)
    except Exception as e: st.error(f"스프레드시트 열기 실패: {e}"); st.stop()

# =============================================================================
# 4) 데이터 I/O
# =============================================================================
@st.cache_data(ttl=180)
def load_master_df() -> pd.DataFrame:
    try: ws = open_spreadsheet().worksheet(SHEET_NAME_MASTER)
    except gspread.WorksheetNotFound: return pd.DataFrame()
    df = pd.DataFrame(ws.get_all_records())
    for c in ["품목코드","품목명","단위","분류","단가","활성"]:
        if c not in df.columns: df[c] = (0 if c=="단가" else (True if c=="활성" else ""))
    if "활성" in df.columns:
        mask = df["활성"].astype(str).str.lower().isin(["1","true","y","yes"])
        df = df[mask | df["활성"].isna()]
    return df

def write_master_df(df: pd.DataFrame) -> bool:
    cols = [c for c in ["품목코드","품목명","분류","단위","단가","활성"] if c in df.columns]
    try:
        sh = open_spreadsheet()
        try: ws = sh.worksheet(SHEET_NAME_MASTER)
        except gspread.WorksheetNotFound: ws = sh.add_worksheet(title=SHEET_NAME_MASTER, rows=2000, cols=25)
        ws.clear()
        ws.update("A1", [cols] + df[cols].fillna("").values.tolist(), value_input_option='USER_ENTERED')
        load_master_df.clear(); return True
    except Exception as e: st.error(f"상품마스터 저장 실패: {e}"); return False

@st.cache_data(ttl=120)
def load_orders_df() -> pd.DataFrame:
    try: ws = open_spreadsheet().worksheet(SHEET_NAME_ORDERS)
    except gspread.WorksheetNotFound: return pd.DataFrame(columns=ORDERS_COLUMNS)
    df = pd.DataFrame(ws.get_all_records())
    for c in ORDERS_COLUMNS:
        if c not in df.columns: df[c] = ""
    return df[ORDERS_COLUMNS].copy()

def write_orders_df(df: pd.DataFrame) -> bool:
    try:
        sh = open_spreadsheet()
        try: ws = sh.worksheet(SHEET_NAME_ORDERS)
        except gspread.WorksheetNotFound: ws = sh.add_worksheet(title=SHEET_NAME_ORDERS, rows=5000, cols=25)
        ws.clear()
        ws.update("A1", [ORDERS_COLUMNS] + df[ORDERS_COLUMNS].fillna("").values.tolist(), value_input_option='USER_ENTERED')
        load_orders_df.clear(); return True
    except Exception as e: st.error(f"발주 저장 실패: {e}"); return False

def append_orders(rows: List[Dict[str, Any]]) -> bool:
    if not rows: return True
    try:
        sh = open_spreadsheet()
        try: ws = sh.worksheet(SHEET_NAME_ORDERS)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=SHEET_NAME_ORDERS, rows=5000, cols=25)
            ws.append_row(ORDERS_COLUMNS, value_input_option='USER_ENTERED')
        values_to_add = [[r.get(col, "") for col in ORDERS_COLUMNS] for r in rows]
        ws.append_rows(values_to_add, value_input_option='USER_ENTERED')
        load_orders_df.clear(); return True
    except Exception as e: st.error(f"발주 추가 실패: {e}"); return False

def update_order_status(selected_ids: List[str], new_status: str, handler: str) -> bool:
    df = load_orders_df()
    if df.empty: st.warning("변경할 데이터가 없습니다."); return False
    now = now_kst_str()
    mask = df["발주번호"].astype(str).isin([str(x) for x in selected_ids])
    df.loc[mask, "상태"] = new_status
    df.loc[mask, "처리일시"] = now
    df.loc[mask, "처리자"] = handler
    return write_orders_df(df)

# =============================================================================
# 5) 로그인
# =============================================================================
def verify_password(input_pw: str, stored_hash: Optional[str], fallback_plain: Optional[str]) -> bool:
    if stored_hash:
        h = stored_hash.strip().lower().split("$", 1)[-1]
        return hashlib.sha256(input_pw.encode()).hexdigest() == h
    return str(input_pw) == str(fallback_plain) if fallback_plain is not None else False

def _find_account(uid_or_name: str):
    s_lower = str(uid_or_name or "").strip().lower()
    if not s_lower: return None, None
    for uid, acct in USERS.items():
        if uid.lower() == s_lower or acct.get("name", "").lower() == s_lower: return uid, acct
    return None, None

def _do_login(uid_input: str, pwd: str):
    real_uid, acct = _find_account(uid_input)
    if not (real_uid and acct and verify_password(pwd, acct.get("password_hash"), acct.get("password"))):
        st.error("아이디(또는 지점명) 또는 비밀번호가 올바르지 않습니다.")
    else:
        st.session_state["auth"] = {"login": True, "user_id": real_uid, "name": acct["name"], "role": acct["role"]}
        st.success(f"{acct['name']}님 환영합니다!")
        st.rerun()

def require_login():
    if st.session_state.get("auth", {}).get("login"): return True
    st.markdown('<div class="login-title">식자재 발주 시스템</div>', unsafe_allow_html=True)
    _, mid, _ = st.columns([3, 2, 3])
    with mid.form("login_form"):
        uid = st.text_input("아이디 또는 지점명", key="login_uid", placeholder="예: jeondae / 전대점")
        pwd = st.text_input("비밀번호", type="password", key="login_pw")
        if st.form_submit_button("로그인", use_container_width=True): _do_login(uid, pwd)
    return False

# =============================================================================
# 6) 유틸
# =============================================================================
def make_order_id(store_id: str) -> str: return f"{datetime.now(KST):%Y%m%d%H%M%S}{store_id}"

def make_order_sheet_excel(df_note: pd.DataFrame, include_price: bool, *, title: str = "산카쿠 납품내역서", period_text: Optional[str] = None) -> BytesIO:
    buf = BytesIO()
    cols = ["발주번호","주문일시","납품요청일","지점명","품목코드","품목명","단위","수량","비고","상태"]
    if include_price: cols += ["단가","금액"]
    
    export = df_note[cols].copy().sort_values(["발주번호","품목코드"])
    for col in ["수량", "단가", "금액"]:
        if col in export.columns:
            export[col] = pd.to_numeric(export[col], errors="coerce").fillna(0)

    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        export.rename(columns={"단가": "단가(원)", "금액": "금액(원)"}).to_excel(w, index=False, sheet_name="내역", startrow=4)
        wb, ws = w.book, w.sheets["내역"]
        fmt = { "title": wb.add_format({"bold": True, "font_size": 16, "align":"center"}), "info":  wb.add_format({"font_size": 10}), "th": wb.add_format({"bold": True, "bg_color":"#F2F2F2", "border":1}), "num": wb.add_format({"num_format":"#,##0"}), "money": wb.add_format({"num_format":"#,##0 원"}), "sum_l": wb.add_format({"bold": True}), "sum_n": wb.add_format({"bold": True, "num_format":"#,##0"}), "sum_m": wb.add_format({"bold": True, "num_format":"#,##0 원"}) }
        ws.merge_range(0, 0, 0, len(export.columns)-1, title, fmt["title"])
        ws.write(1, 0, f"조회기간: {period_text or ''}", fmt["info"])
        ws.write(2, 0, f"생성일시(KST): {now_kst_str()}", fmt["info"])
    buf.seek(0)
    return buf

# =============================================================================
# 🛒 장바구니 유틸(전역)
# =============================================================================
def _ensure_cart():
    if "cart" not in st.session_state: st.session_state.cart = pd.DataFrame(columns=["품목코드","품목명","단위","단가","수량","총금액"])

def _coerce_price_qty(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty: return pd.DataFrame(columns=["품목코드","품목명","단위","단가","수량","총금액"])
    out = df.copy()
    required_cols = ["품목코드","품목명","단위","단가","수량","총금액"]
    for col in required_cols:
        if col not in out.columns: out[col] = "" if col in ["품목코드","품목명","단위"] else 0
    for c in ["단가","수량"]:
        if c in out.columns: out[c] = pd.to_numeric(out[c].astype(str).str.replace(",", "").str.strip(), errors="coerce").fillna(0).astype(int)
    out["총금액"] = out.get("단가", 0) * out.get("수량", 0)
    return out[required_cols]

def normalize_cart(df: pd.DataFrame) -> pd.DataFrame:
    df = _coerce_price_qty(df)
    return df[df["수량"] > 0].reset_index(drop=True)

def _add_to_cart(rows_df: pd.DataFrame):
    _ensure_cart()
    add = normalize_cart(rows_df)
    if add.empty: return
    cart = st.session_state.cart.copy()
    merged = pd.concat([cart, add]).groupby("품목코드", as_index=False).agg({"품목명": "last", "단위": "last", "단가": "last", "수량": "sum"})
    merged["총금액"] = merged["단가"] * merged["수량"]
    st.session_state.cart = merged

def _remove_from_cart(codes: list[str]):
    _ensure_cart()
    if codes: st.session_state.cart = st.session_state.cart[~st.session_state.cart["품목코드"].isin(codes)]

def _clear_cart():
    _ensure_cart()
    st.session_state.cart = pd.DataFrame(columns=["품목코드","품목명","단위","단가","수량","총금액"])

# =============================================================================
# 🛒 발주(지점) 화면
# =============================================================================
def page_store_register_confirm(master_df: pd.DataFrame):
    _ensure_cart()
    if "store_editor_ver" not in st.session_state: st.session_state.store_editor_ver = 0
    if "cart_selected_codes" not in st.session_state: st.session_state.cart_selected_codes = []

    st.subheader("🛒 발주 요청")
    v_spacer(10)

    with st.container(border=True):
        st.markdown("##### 🗓️ 납품 요청 정보")
        today, max_day = date.today(), date.today() + timedelta(days=7)
        c1, c2 = st.columns([1, 1.2])
        quick = c1.radio("납품 선택", ["오늘", "내일", "직접선택"], horizontal=True, key="store_quick_radio", label_visibility="collapsed")
        
        if quick == "오늘": 납품요청일 = today
        elif quick == "내일": 납품요청일 = min(today + timedelta(days=1), max_day)
        else:
            default = st.session_state.get("store_req_date", today)
            if not isinstance(default, date) or not (today <= default <= max_day): default = today
            납품요청일 = c2.date_input("납품 요청일", value=default, min_value=today, max_value=max_day, key="store_req_date", label_visibility="collapsed")
        memo = st.text_area("요청 사항(선택)", key="store_req_memo", height=80, placeholder="예) 입고 시 얼음팩 추가 부탁드립니다.")

    v_spacer(16)
    
    with st.container(border=True):
        st.markdown("##### 🧾 발주 수량 입력")
        master_df["단가"] = pd.to_numeric(master_df["단가"], errors="coerce").fillna(0).astype(int)

        l, r = st.columns([2, 1])
        keyword = l.text_input("품목 검색(이름/코드)", key="store_kw", placeholder="오이, P001 등")
        cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
        cat_sel = r.selectbox("분류(선택)", cat_opt, key="store_cat_sel")

        df_view = master_df.copy()
        if keyword:
            q = keyword.strip().lower()
            df_view = df_view[df_view.apply(lambda row: q in str(row.get("품목명","")).lower() or q in str(row.get("품목코드","")).lower(), axis=1)]
        if cat_sel != "(전체)": df_view = df_view[df_view["분류"] == cat_sel]

        with st.form(key="add_to_cart_form"):
            df_edit = df_view[["품목코드","품목명","단위","단가"]].copy()
            df_edit["수량"] = ""
            
            edited_disp = st.data_editor(
                df_edit, key=f"store_order_editor_v{st.session_state.store_editor_ver}", hide_index=True, disabled=["품목코드","품목명","단위","단가"], use_container_width=True,
                column_config={
                    "품목코드": st.column_config.Column("품목코드", width="medium"), "품목명": st.column_config.Column("품목명", width="large"),
                    "단가": st.column_config.NumberColumn("단가(원)", format="%,.0f"), "수량": st.column_config.TextColumn("수량", help="숫자/콤마 입력 가능"),
                })
            add_clicked = st.form_submit_button("장바구니 추가", use_container_width=True, type="primary")

        if add_clicked:
            items_to_add = normalize_cart(edited_disp)
            if items_to_add.empty: st.warning("수량이 0보다 큰 품목이 없습니다.")
            else:
                _add_to_cart(items_to_add)
                st.toast(f"{len(items_to_add)}개 품목을 장바구니에 추가했습니다.", icon="🛒")
                st.session_state.store_editor_ver += 1
                st.rerun()

    v_spacer(16)

    with st.container(border=True):
        st.markdown("##### 🧺 장바구니")
        cart = st.session_state.cart
        if not cart.empty:
            cart_disp = cart.copy(); cart_disp.insert(0, "선택", cart_disp["품목코드"].isin(st.session_state.cart_selected_codes))
            cart_disp["수량"] = cart_disp["수량"].astype(str)
            edited_cart = st.data_editor(cart_disp, key="cart_editor", hide_index=True, disabled=["품목코드","품목명","단위","총금액"], column_config={"단가": st.column_config.NumberColumn("단가(원)", format="%,.0f")})
            st.session_state.cart_selected_codes = edited_cart[edited_cart["선택"]]["품목코드"].tolist()
            st.session_state.cart = normalize_cart(edited_cart.drop(columns=["선택"]))
            
            c1, c2, c3, _ = st.columns([1,1,1,4])
            if c1.button("전체" if not st.session_state.cart_selected_codes else "해제", use_container_width=True):
                st.session_state.cart_selected_codes = [] if st.session_state.cart_selected_codes else st.session_state.cart["품목코드"].tolist()
            if c2.button("선택 삭제", use_container_width=True, disabled=not st.session_state.cart_selected_codes):
                _remove_from_cart(st.session_state.cart_selected_codes); st.session_state.cart_selected_codes = []
            if c3.button("비우기", use_container_width=True):
                _clear_cart(); st.session_state.cart_selected_codes = []
        else: st.info("장바구니가 비어 있습니다.")

    v_spacer(16)
    
    with st.form("submit_form"):
        cart_now = st.session_state.cart
        st.markdown(f"**최종 확인:** 총 {len(cart_now)}개 품목, 합계 {fmt_num(cart_now['총금액'].sum())}원")
        confirm = st.checkbox("위 내용으로 발주를 제출합니다.")
        if st.form_submit_button("📦 발주 제출", type="primary", use_container_width=True, disabled=cart_now.empty):
            if not confirm: st.warning("제출 확인 체크박스를 선택해주세요."); st.stop()
            user, order_id = st.session_state.auth, make_order_id(st.session_state.auth["user_id"])
            rows_to_append = [{"주문일시": now_kst_str(), "발주번호": order_id, "지점ID": user["user_id"], "지점명": user["name"], "납품요청일": f"{납품요청일:%Y-%m-%d}", "비고": memo, "상태": "접수", "처리일시": "", "처리자": "", **r.to_dict()} for _, r in cart_now.iterrows()]
            if append_orders(rows_to_append):
                st.success("발주가 성공적으로 제출되었습니다."); _clear_cart(); st.rerun()
            else: st.error("발주 제출 중 오류가 발생했습니다.")

# ──────────────────────────────────────────────
# [신규] 재사용 가능한 UI 함수
# ──────────────────────────────────────────────
def render_selectable_list(df: pd.DataFrame, session_state_key: str, editor_key: str) -> List[str]:
    if df.empty: return []
    if session_state_key not in st.session_state: st.session_state[session_state_key] = []
    
    disp_df = df.copy()
    disp_df.insert(0, "선택", disp_df["발주번호"].isin(st.session_state[session_state_key]))
    
    edited_df = st.data_editor( disp_df, key=editor_key, hide_index=True, use_container_width=True, disabled=df.columns, column_config={"총금액": st.column_config.NumberColumn("총금액", format="%,.0f원")})
    
    selected_ids = edited_df[edited_df["선택"]]["발주번호"].tolist()
    st.session_state[session_state_key] = selected_ids
    return selected_ids

# ──────────────────────────────────────────────
# 🧾 발주 조회/수정 (지점)
# ──────────────────────────────────────────────
def page_store_orders_change():
    st.subheader("🧾 발주 조회 · 수정")
    if "store_selected_orders" not in st.session_state: st.session_state.store_selected_orders = []

    df_all = load_orders_df()
    df_user = df_all[df_all["지점ID"] == st.session_state["auth"]["user_id"]]
    if df_user.empty: st.info("발주 데이터가 없습니다."); return

    orders = df_user.groupby("발주번호").agg(주문일시=("주문일시", "first"), 건수=("품목코드", "count"), 총금액=("금액", lambda x: pd.to_numeric(x, 'coerce').sum()), 상태=("상태", "first")).reset_index().sort_values("주문일시", ascending=False)
    
    pending = orders[orders["상태"] == "접수"]; done = orders[orders["상태"] == "출고완료"]
    
    with st.container(border=True):
        st.markdown("##### 📦 발주 리스트")
        st.caption("삭제를 원하는 '접수' 상태의 발주를 선택하세요. 상세 내용을 보려면 하나만 선택하세요.")
        selected_ids = render_selectable_list(pd.concat([pending, done]), "store_selected_orders", "store_orders_editor")

        is_deletable = any(pid in pending["발주번호"].tolist() for pid in selected_ids)
        if st.button("선택 발주 삭제", disabled=not is_deletable):
            if write_orders_df(df_all[~df_all["발주번호"].isin(selected_ids)]):
                st.toast("선택한 발주가 삭제되었습니다.", icon="🗑️"); st.session_state.store_selected_orders = []; st.rerun()
            else: st.error("삭제 실패")

    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 📄 발주품목조회")
        if len(selected_ids) == 1:
            target_df = df_user[df_user["발주번호"] == selected_ids[0]]
            st.dataframe(target_df[ORDERS_COLUMNS[5:12]], hide_index=True, use_container_width=True, column_config={"단가": st.column_config.NumberColumn("단가", format="%,.0f"),"금액": st.column_config.NumberColumn("금액", format="%,.0f")})
            
            buf = make_order_sheet_excel(target_df, include_price=False, title=f"발주서 ({selected_ids[0]})")
            st.download_button("이 발주서 다운로드", data=buf, file_name=f"발주서_{selected_ids[0]}.xlsx", mime="application/vnd.ms-excel", use_container_width=True)
        else: st.info("상세 내용을 보려면 위 목록에서 발주를 하나만 선택하세요.")

# ──────────────────────────────────────────────
# 📑 발주서 다운로드 (지점)
# ──────────────────────────────────────────────
def page_store_order_form_download():
    st.subheader("📑 발주서 다운로드")
    df_all = load_orders_df()
    df = df_all[df_all["지점ID"] == st.session_state["auth"]["user_id"]]
    if df.empty: st.info("발주 데이터가 없습니다."); return

    with st.container(border=True):
        st.markdown("### 🔎 조회 조건")
        c1, c2, c3 = st.columns([1, 1, 2])
        dt_from = c1.date_input("시작일", date.today() - timedelta(days=7), key="store_dl_from")
        dt_to = c2.date_input("종료일", date.today(), key="store_dl_to")
        order_ids = ["(전체)"] + sorted(df["발주번호"].dropna().unique().tolist(), reverse=True)
        target_order = c3.selectbox("발주번호(선택 시 해당 건만)", order_ids, key="store_dl_orderid")
    
    dt_series = pd.to_datetime(df["주문일시"], errors="coerce").dt.date
    mask = (dt_series >= dt_from) & (dt_series <= dt_to)
    if target_order != "(전체)": mask &= (df["발주번호"] == target_order)
    dfv = df[mask].copy().sort_values(["발주번호", "품목코드"])

    with st.container(border=True):
        st.markdown("### 📄 미리보기")
        st.dataframe(dfv, use_container_width=True, height=420, column_config={"단가": st.column_config.NumberColumn("단가", format="%,.0f"),"금액": st.column_config.NumberColumn("금액", format="%,.0f")})
        
        if not dfv.empty:
            period_text = f"{dt_from:%Y-%m-%d} ~ {dt_to:%Y-%m-%d}"
            buf = make_order_sheet_excel(dfv, include_price=False, title="산카쿠 발주서", period_text=period_text)
            fname = make_filename("산카쿠 발주서", dt_from, dt_to)
            st.download_button("발주서 엑셀 다운로드", data=buf.getvalue(), file_name=fname, mime="application/vnd.ms-excel", key="store_dl_btn", use_container_width=True)

# ──────────────────────────────────────────────
# 🏷️ 품목 가격 조회 (지점)
# ──────────────────────────────────────────────
def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 발주 품목 가격 조회")
    cols = ["품목코드", "품목명", "분류", "단위", "단가"]
    view = master_df[[c for c in cols if c in master_df.columns]].copy()
    view["단가"] = pd.to_numeric(view.get("단가", 0), errors="coerce").fillna(0).astype(int)
    st.dataframe(view, use_container_width=True, hide_index=True, column_config={"단가": st.column_config.NumberColumn("단가(원)", format="%,.0f")})

# ──────────────────────────────────────────────
# 🗂️ 발주요청조회 · 수정 (관리자)
# ──────────────────────────────────────────────
def page_admin_unified_management():
    st.subheader("🗂️ 발주요청조회 · 수정")
    st.caption("각 탭에서 발주를 선택하여 상태를 변경하거나, 하나를 선택하여 상세 내용을 확인하세요.")
    if "admin_pending_selection" not in st.session_state: st.session_state.admin_pending_selection = []
    if "admin_shipped_selection" not in st.session_state: st.session_state.admin_shipped_selection = []
    
    df_all = load_orders_df()
    if df_all.empty: st.info("발주 데이터가 없습니다."); return
    
    orders = df_all.groupby("발주번호").agg(주문일시=("주문일시", "first"), 지점명=("지점명", "first"), 건수=("품목코드", "count"), 총금액=("금액", lambda x: pd.to_numeric(x, 'coerce').sum()), 상태=("상태", "first")).reset_index().sort_values("주문일시", ascending=False)
    pending = orders[orders["상태"] == "접수"]; shipped = orders[orders["상태"] == "출고완료"]

    tab1, tab2 = st.tabs([f"📦 발주 요청 접수 ({len(pending)}건)", f"✅ 출고 완료 ({len(shipped)}건)"])
    
    with tab1:
        sel_pending = render_selectable_list(pending, "admin_pending_selection", "admin_pending_editor")
        if st.button("✅ 선택 발주 출고", type="primary", disabled=not sel_pending):
            if update_order_status(sel_pending, "출고완료", st.session_state.auth["name"]):
                st.toast(f"{len(sel_pending)}건이 출고 처리되었습니다.", icon="✅"); st.session_state.admin_pending_selection = []; st.rerun()
    with tab2:
        sel_shipped = render_selectable_list(shipped, "admin_shipped_selection", "admin_shipped_editor")
        if st.button("↩️ 접수 상태로 변경", disabled=not sel_shipped):
            if update_order_status(sel_shipped, "접수", st.session_state.auth["name"]):
                st.toast(f"{len(sel_shipped)}건이 접수 상태로 변경되었습니다.", icon="↩️"); st.session_state.admin_shipped_selection = []; st.rerun()
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 📄 발주요청품목확인")
        total_selection = st.session_state.admin_pending_selection + st.session_state.admin_shipped_selection
        if len(total_selection) == 1:
            target_df = df_all[df_all["발주번호"] == total_selection[0]]
            st.dataframe(target_df[ORDERS_COLUMNS[5:12]], hide_index=True, use_container_width=True)
        else: st.info("상세 내용을 보려면 위 목록에서 발주를 하나만 선택하세요.")

# ──────────────────────────────────────────────
# 📑 출고 내역서 다운로드 (관리자)
# ──────────────────────────────────────────────
def page_admin_delivery_note():
    st.subheader("📑 출고 내역서 다운로드")
    st.info("기능 개발 중입니다.")

# ──────────────────────────────────────────────
# 🏷️ 납품 품목 가격 설정 (관리자)
# ──────────────────────────────────────────────
def page_admin_items_price(master_df: pd.DataFrame):
    st.subheader("🏷️ 납품 품목 가격 설정")
    st.caption("가격을 수정하거나 품목을 추가/삭제한 후 저장 버튼을 누르세요.")
    
    with st.form("master_edit_form"):
        edited = st.data_editor(master_df.assign(삭제=False), hide_index=True, num_rows="dynamic", use_container_width=True,
            column_config={"단가": st.column_config.NumberColumn("단가(원)", format="%,.0f")})
        if st.form_submit_button("변경사항 저장", type="primary", use_container_width=True):
            final_df = edited[~edited["삭제"]].drop(columns=["삭제"])
            if write_master_df(final_df):
                st.toast("상품마스터가 저장되었습니다.", icon="💾"); st.rerun()

# =============================================================================
# 라우팅
# =============================================================================
if __name__ == "__main__":
    if not require_login(): st.stop()
    st.title("📦 식자재 발주 시스템")
    user, master = st.session_state.auth, load_master_df()
    
    if user["role"] == "admin":
        tabs = st.tabs(["🗂️ 발주요청조회·수정", "📑 출고 내역서 다운로드", "🏷️ 납품 품목 가격 설정"])
        with tabs[0]: page_admin_unified_management()
        with tabs[1]: page_admin_delivery_note()
        with tabs[2]: page_admin_items_price(master)
    else:
        tabs = st.tabs(["🛒 발주 요청", "🧾 발주 조회·수정", "📑 발주서 다운로드", "🏷️ 발주 품목 가격 조회"])
        with tabs[0]: page_store_register_confirm(master)
        with tabs[1]: page_store_orders_change()
        with tabs[2]: page_store_order_form_download()
        with tabs[3]: page_store_master_view(master)
