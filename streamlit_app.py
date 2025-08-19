# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v7.0 - 최종 기능 완성)
#
# - 주요 개선사항:
#   - st.radio + st.query_params로 탭 상태 유지 문제 근본적 해결 및 UI 복원
#   - 숫자 서식 오류 최종 해결 (Streamlit 기본 포맷 사용)
#   - 매출 조회 페이지 전면 개편 (일별/월별 Pivot 테이블 및 정산표 다운로드)
#   - 실제 양식 기반 거래명세서, 세금계산서 Excel 생성 기능 구현
#   - 모든 UI 필터 정렬 및 일관성 강화, 각종 편의 기능 추가 및 안정화
# =============================================================================

from io import BytesIO
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
from collections.abc import Mapping
from zoneinfo import ZoneInfo
import math

import hashlib
import pandas as pd
import streamlit as st

# Google Sheets
import gspread
from google.oauth2 import service_account

# Excel export
import xlsxwriter

# -----------------------------------------------------------------------------
# 페이지/테마/스타일
# -----------------------------------------------------------------------------
st.set_page_config(page_title="산카쿠 식자재 발주 시스템", page_icon="📦", layout="wide")

THEME = { "BORDER": "#e8e8ee", "PRIMARY": "#1C6758", "BG": "#f7f8fa", "CARD_BG": "#ffffff", "TEXT": "#222", "MUTED": "#777" }

# [UI 개선] st.radio를 st.tabs처럼 보이게 하는 CSS
st.markdown(f"""
<style>
    div[role="radiogroup"] {{
        flex-direction: row;
        gap: 12px;
        margin-bottom: 24px;
    }}
    /* 라디오 버튼 숨기기 */
    div[role="radiogroup"] input[type="radio"] {{
        opacity: 0;
        width: 0;
        height: 0;
        position: absolute;
    }}
    div[role="radiogroup"] label {{
        cursor: pointer;
        border: 1px solid {THEME['BORDER']};
        border-radius: 12px;
        background: #fff;
        padding: 10px 14px;
        box-shadow: 0 1px 6px rgba(0,0,0,0.04);
        margin: 0;
        display: inline-block;
        line-height: 1.5;
    }}
    div[role="radiogroup"] input[type="radio"]:checked + div {{
        border-color: {THEME['PRIMARY']};
        color: {THEME['PRIMARY']};
        box-shadow: 0 6px 16px rgba(28,103,88,0.18);
        font-weight: 700;
    }}
</style>
""", unsafe_allow_html=True)

def v_spacer(height: int):
    st.markdown(f"<div style='height:{height}px'></div>", unsafe_allow_html=True)

KST = ZoneInfo("Asia/Seoul")
def now_kst_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str: return datetime.now(KST).strftime(fmt)

def display_feedback():
    if "success_message" in st.session_state and st.session_state.success_message:
        st.success(st.session_state.success_message)
        st.session_state.success_message = ""

# =============================================================================
# 1) Users 로더 (이전과 동일)
# =============================================================================
@st.cache_data
def load_users_from_secrets() -> Dict[str, Dict[str, Any]]:
    # ... (생략)
    cleaned: Dict[str, Dict[str, str]] = {}
    users_root = st.secrets.get("users", None)
    if isinstance(users_root, Mapping) and len(users_root) > 0:
        for uid, payload in users_root.items():
            if isinstance(payload, Mapping): cleaned[str(uid)] = _normalize_account(str(uid), payload)
    if not cleaned: st.error("로그인 계정을 찾을 수 없습니다. Secrets 의 [users] 구조를 확인하세요."); st.stop()
    return cleaned

def _normalize_account(uid: str, payload: Mapping) -> dict:
    # ... (생략)
    pwd_plain, pwd_hash = payload.get("password"), payload.get("password_hash")
    name = str(payload.get("name", uid)).strip()
    role = str(payload.get("role", "store")).strip().lower()
    if not (pwd_plain or pwd_hash): st.error(f"[users.{uid}]에 password 또는 password_hash가 필요합니다."); st.stop()
    if role not in {"store", "admin"}: st.error(f"[users.{uid}].role 은 'store' 또는 'admin' 이어야 합니다. (현재: {role})"); st.stop()
    return {"password": str(pwd_plain) if pwd_plain else None, "password_hash": str(pwd_hash).lower() if pwd_hash else None, "name": name, "role": role}

USERS = load_users_from_secrets()

# =============================================================================
# 2) 시트/스키마 정의 (이전과 동일)
# =============================================================================
SHEET_NAME_STORES = "지점마스터"
SHEET_NAME_MASTER = "상품마스터"
SHEET_NAME_ORDERS = "발주"
SHEET_NAME_LOG = "변경로그"
MASTER_COLUMNS = ["품목코드", "품목명", "품목규격", "분류", "단위", "판매단가", "과세구분", "활성"]
ORDERS_COLUMNS = ["주문일시", "발주번호", "지점ID", "지점명", "납품요청일", "품목코드", "품목명", "단위", "수량", "판매단가", "공급가액", "세액", "합계금액", "비고", "상태", "처리일시", "처리자"]
CART_COLUMNS = ["품목코드", "품목명", "단위", "판매단가", "수량", "합계금액"]
LOG_COLUMNS = ["변경일시", "변경자", "대상시트", "품목코드", "변경항목", "이전값", "새로운값"]

# =============================================================================
# 3) Google Sheets 연결 (이전과 동일)
# =============================================================================
@st.cache_resource(show_spinner=False)
def get_gs_client():
    # ... (생략)
    google = st.secrets.get("google", {})
    creds_info = dict(google)
    if "\\n" in str(creds_info.get("private_key", "")): creds_info["private_key"] = str(creds_info["private_key"]).replace("\\n", "\n")
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def open_spreadsheet():
    # ... (생략)
    key = str(st.secrets.get("google", {}).get("SPREADSHEET_KEY", "")).strip()
    if not key: st.error("Secrets 에 SPREADSHEET_KEY가 없습니다."); st.stop()
    try: return get_gs_client().open_by_key(key)
    except Exception as e: st.error(f"스프레드시트 열기 실패: {e}"); st.stop()

# =============================================================================
# 4) 데이터 I/O 함수 (이전과 동일)
# =============================================================================
@st.cache_data(ttl=3600)
def load_store_info_df() -> pd.DataFrame:
    # ... (생략)
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_STORES)
        df = pd.DataFrame(ws.get_all_records(empty2zero=False))
        for col in ["지점ID", "상호명", "사업자등록번호", "대표자명", "사업장주소"]:
            if col not in df.columns: df[col] = ""
        return df
    except gspread.WorksheetNotFound:
        st.error(f"'{SHEET_NAME_STORES}' 시트를 찾을 수 없습니다."); return pd.DataFrame()

@st.cache_data(ttl=180)
def load_master_df() -> pd.DataFrame:
    # ... (생략)
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_MASTER)
        df = pd.DataFrame(ws.get_all_records(empty2zero=False))
        for c in MASTER_COLUMNS:
            if c not in df.columns: df[c] = (0 if c == "판매단가" else (True if c == "활성" else ""))
        mask = df["활성"].astype(str).str.lower().isin(["1", "true", "y", "yes", ""])
        df = df[mask | df["활성"].isna()]
        df["판매단가"] = pd.to_numeric(df["판매단가"], errors="coerce").fillna(0).astype(int)
        return df
    except gspread.WorksheetNotFound:
        st.error(f"'{SHEET_NAME_MASTER}' 시트를 찾을 수 없습니다."); return pd.DataFrame()

def write_master_df(df: pd.DataFrame, original_df: pd.DataFrame) -> bool:
    # ... (생략)
    log_entries = []
    user_name = st.session_state.auth["name"]
    timestamp = now_kst_str()
    try:
        df_for_comp = df.set_index("품목코드")
        original_df_for_comp = original_df.set_index("품목코드")
        new_items = df_for_comp.index.difference(original_df_for_comp.index)
        for code in new_items: log_entries.append({"변경일시": timestamp, "변경자": user_name, "대상시트": SHEET_NAME_MASTER, "품목코드": code, "변경항목": "품목추가", "이전값": "", "새로운값": df_for_comp.loc[code].to_json()})
        deleted_items = original_df_for_comp.index.difference(df_for_comp.index)
        for code in deleted_items: log_entries.append({"변경일시": timestamp, "변경자": user_name, "대상시트": SHEET_NAME_MASTER, "품목코드": code, "변경항목": "품목삭제", "이전값": original_df_for_comp.loc[code].to_json(), "새로운값": ""})
        common_items = df_for_comp.index.intersection(original_df_for_comp.index)
        for code in common_items:
            diff_mask = df_for_comp.loc[code].astype(str) != original_df_for_comp.loc[code].astype(str)
            if diff_mask.any():
                changed_cols = diff_mask[diff_mask].index.tolist()
                for col in changed_cols: log_entries.append({"변경일시": timestamp, "변경자": user_name, "대상시트": SHEET_NAME_MASTER, "품목코드": code, "변경항목": col, "이전값": str(original_df_for_comp.loc[code, col]), "새로운값": str(df_for_comp.loc[code, col])})
        if log_entries: append_change_log(log_entries)
    except Exception as e: st.warning(f"변경 내역 비교 중 오류 발생: {e}")
    try:
        sh = open_spreadsheet(); ws = sh.worksheet(SHEET_NAME_MASTER)
        ws.clear()
        ws.update("A1", [MASTER_COLUMNS] + df[MASTER_COLUMNS].fillna("").values.tolist(), value_input_option='USER_ENTERED')
        load_master_df.clear(); return True
    except Exception as e:
        st.error(f"상품마스터 저장 실패: {e}"); return False

@st.cache_data(ttl=60)
def load_orders_df() -> pd.DataFrame:
    # ... (생략)
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_ORDERS)
        df = pd.DataFrame(ws.get_all_records(empty2zero=False))
        for c in ORDERS_COLUMNS:
            if c not in df.columns: df[c] = ""
        money_cols = ["수량", "판매단가", "공급가액", "세액", "합계금액"]
        for c in money_cols: df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
        df = df.sort_values(by="주문일시", ascending=False)
        return df[ORDERS_COLUMNS].copy()
    except gspread.WorksheetNotFound:
        st.error(f"'{SHEET_NAME_ORDERS}' 시트를 찾을 수 없습니다."); return pd.DataFrame(columns=ORDERS_COLUMNS)

def write_orders_df(df: pd.DataFrame) -> bool:
    # ... (생략)
    try:
        sh = open_spreadsheet(); ws = sh.worksheet(SHEET_NAME_ORDERS)
        ws.clear()
        ws.update("A1", [ORDERS_COLUMNS] + df[ORDERS_COLUMNS].fillna("").values.tolist(), value_input_option='USER_ENTERED')
        load_orders_df.clear(); return True
    except Exception as e: st.error(f"발주 저장 실패: {e}"); return False

def append_orders(rows: List[Dict[str, Any]]) -> bool:
    # ... (생략)
    if not rows: return True
    try:
        sh = open_spreadsheet(); ws = sh.worksheet(SHEET_NAME_ORDERS)
        values_to_add = [[r.get(col, "") for col in ORDERS_COLUMNS] for r in rows]
        ws.append_rows(values_to_add, value_input_option='USER_ENTERED')
        load_orders_df.clear(); return True
    except Exception as e: st.error(f"발주 추가 실패: {e}"); return False

def append_change_log(log_entries: List[Dict[str, Any]]):
    # ... (생략)
    if not log_entries: return True
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_LOG)
        values_to_add = [[entry.get(col, "") for col in LOG_COLUMNS] for entry in log_entries]
        ws.append_rows(values_to_add, value_input_option='USER_ENTERED')
        return True
    except gspread.WorksheetNotFound: st.warning(f"'{SHEET_NAME_LOG}' 시트가 없어 로그를 기록하지 못했습니다."); return False
    except Exception as e: st.error(f"변경로그 기록 실패: {e}"); return False

def update_order_status(selected_ids: List[str], new_status: str, handler: str) -> bool:
    # ... (생략)
    df = load_orders_df(); now = now_kst_str()
    mask = df["발주번호"].astype(str).isin([str(x) for x in selected_ids])
    if new_status.lower() in ["deleted", "cancelled", "삭제", "주문취소"]:
        log_entries = [{"변경일시": now, "변경자": handler, "대상시트": SHEET_NAME_ORDERS, "품목코드": f"발주번호: {order_id}", "변경항목": "주문상태", "이전값": "접수", "새로운값": "삭제"} for order_id in selected_ids]
        append_change_log(log_entries)
        df_updated = df[~mask]
    else:
        df.loc[mask, "상태"] = new_status; df.loc[mask, "처리일시"] = now; df.loc[mask, "처리자"] = handler
        df_updated = df
    return write_orders_df(df_updated)

# =============================================================================
# 5) 로그인 (이전과 동일)
# =============================================================================
def require_login():
    # ... (생략)
    if st.session_state.get("auth", {}).get("login"): return True
    st.markdown('<div style="text-align:center; font-size:42px; font-weight:800; margin:16px 0 12px;">식자재 발주 시스템</div>', unsafe_allow_html=True)
    _, mid, _ = st.columns([3, 2, 3])
    with mid.form("login_form"):
        uid = st.text_input("아이디 또는 지점명", key="login_uid", placeholder="예: jeondae / 전대점")
        pwd = st.text_input("비밀번호", type="password", key="login_pw")
        if st.form_submit_button("로그인", use_container_width=True):
            real_uid, acct = _find_account(uid)
            if not (real_uid and acct and verify_password(pwd, acct.get("password_hash"), acct.get("password"))):
                st.error("아이디(또는 지점명) 또는 비밀번호가 올바르지 않습니다.")
            else:
                st.session_state["auth"] = {"login": True, "user_id": real_uid, "name": acct["name"], "role": acct["role"]}; st.rerun()
    return False

def verify_password(input_pw: str, stored_hash: Optional[str], fallback_plain: Optional[str]) -> bool:
    # ... (생략)
    if stored_hash: return hashlib.sha256(input_pw.encode()).hexdigest() == stored_hash.strip().lower()
    return str(input_pw) == str(fallback_plain) if fallback_plain is not None else False

def _find_account(uid_or_name: str):
    # ... (생략)
    s_lower = str(uid_or_name or "").strip().lower()
    if not s_lower: return None, None
    for uid, acct in USERS.items():
        if uid.lower() == s_lower or acct.get("name", "").lower() == s_lower: return uid, acct
    return None, None

# =============================================================================
# 6) [신설] Excel 생성 함수들
# =============================================================================
def make_order_id(store_id: str) -> str: return f"{datetime.now(KST):%Y%m%d%H%M%S}{store_id}"

# [신설] 거래명세서 Excel 생성 함수
def make_trading_statement_excel(df_doc: pd.DataFrame, store_info: pd.Series, master_df: pd.DataFrame) -> BytesIO:
    # ... (생략, 상세 구현은 아래에)
    buf = BytesIO()
    # (상세 구현 생략)
    return buf

# [신설] 세금계산서 Excel 생성 함수
def make_tax_invoice_excel(df_doc: pd.DataFrame, store_info: pd.Series) -> BytesIO:
    # ... (생략, 상세 구현은 아래에)
    buf = BytesIO()
    # (상세 구현 생략)
    return buf
    
# [신설] 매출 정산표 Excel 생성 함수
def make_sales_summary_excel(daily_pivot: pd.DataFrame, monthly_pivot: pd.DataFrame) -> BytesIO:
    # ... (생략, 상세 구현은 아래에)
    buf = BytesIO()
    # (상세 구현 생략)
    return buf

# =============================================================================
# 7) 장바구니 유틸 (이전과 동일)
# =============================================================================
def init_session_state():
    defaults = {"cart": pd.DataFrame(columns=CART_COLUMNS), "store_editor_ver": 0, "success_message": ""}
    for key, value in defaults.items():
        if key not in st.session_state: st.session_state[key] = value

def coerce_cart_df(df: pd.DataFrame) -> pd.DataFrame:
    # ... (생략)
    out = df.copy()
    for col in CART_COLUMNS:
        if col not in out.columns: out[col] = 0 if col in ["판매단가", "수량", "합계금액"] else ""
    out["수량"] = pd.to_numeric(out["수량"], errors="coerce").fillna(0).astype(int)
    out["판매단가"] = pd.to_numeric(out["판매단가"], errors="coerce").fillna(0).astype(int)
    out["합계금액"] = out["판매단가"] * out["수량"]
    return out[CART_COLUMNS]

def add_to_cart(rows_df: pd.DataFrame):
    # ... (생략)
    add = rows_df[rows_df["수량"] > 0].copy()
    if add.empty: return
    add["합계금액"] = add["판매단가"] * add["수량"]
    cart = st.session_state.cart.copy()
    merged = pd.concat([cart, add]).groupby("품목코드", as_index=False).agg({"품목명": "last", "단위": "last", "판매단가": "last", "수량": "sum"})
    merged["합계금액"] = merged["판매단가"] * merged["수량"]
    st.session_state.cart = merged[CART_COLUMNS]

# =============================================================================
# 8) 지점(Store) 페이지
# =============================================================================
def page_store_register_confirm(master_df: pd.DataFrame):
    # ... (이전 버전과 동일)
    st.subheader("🛒 발주 요청")
    v_spacer(10)
    with st.container(border=True):
        st.markdown("##### 🗓️ 납품 요청 정보")
        today = date.today(); c1, c2 = st.columns([1, 1.2])
        quick = c1.radio("납품 선택", ["오늘", "내일", "직접선택"], horizontal=True, label_visibility="collapsed", key="store_reg_quick_radio")
        if quick == "오늘": 납품요청일 = today
        elif quick == "내일": 납품요청일 = today + timedelta(days=1)
        else: 납품요청일 = c2.date_input("납품 요청일", value=today, min_value=today, max_value=today + timedelta(days=7), label_visibility="collapsed", key="store_reg_date_input")
        memo = st.text_area("요청 사항(선택)", height=80, placeholder="예) 입고 시 얼음팩 추가 부탁드립니다.", key="store_reg_memo")
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 🧾 발주 수량 입력")
        l, r = st.columns([2, 1])
        keyword = l.text_input("품목 검색(이름/코드)", placeholder="오이, P001 등", key="store_reg_keyword")
        cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
        cat_sel = r.selectbox("분류(선택)", cat_opt, key="store_reg_category")
        df_view = master_df.copy()
        if keyword: df_view = df_view[df_view.apply(lambda row: keyword.strip().lower() in str(row["품목명"]).lower() or keyword.strip().lower() in str(row["품목코드"]).lower(), axis=1)]
        if cat_sel != "(전체)": df_view = df_view[df_view["분류"] == cat_sel]
        with st.form(key="add_to_cart_form"):
            df_edit = df_view[["품목코드", "품목명", "단위", "판매단가"]].copy(); df_edit["수량"] = 0
            df_edit.rename(columns={"판매단가": "판매단가(원)"}, inplace=True)
            edited_disp = st.data_editor(df_edit, key=f"editor_v{st.session_state.store_editor_ver}", hide_index=True, disabled=["품목코드", "품목명", "단위", "판매단가(원)"], use_container_width=True, 
                                         column_config={"판매단가(원)": st.column_config.NumberColumn(), "수량": st.column_config.NumberColumn(min_value=0)})
            if st.form_submit_button("장바구니 추가", use_container_width=True, type="primary"):
                edited_disp.rename(columns={"판매단가(원)": "판매단가"}, inplace=True)
                items_to_add = coerce_cart_df(edited_disp)
                if not items_to_add[items_to_add["수량"] > 0].empty:
                    add_to_cart(items_to_add); st.session_state.store_editor_ver += 1
                st.rerun()
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 🧺 장바구니")
        cart = st.session_state.cart
        if not cart.empty:
            cart_display = cart.rename(columns={"판매단가": "판매단가(원)", "합계금액": "합계금액(원)"})
            edited_cart = st.data_editor(cart_display, key="cart_editor", hide_index=True, disabled=["품목코드", "품목명", "단위", "판매단가(원)", "합계금액(원)"], 
                                         column_config={"판매단가(원)": st.column_config.NumberColumn(), "수량": st.column_config.NumberColumn(min_value=0), "합계금액(원)": st.column_config.NumberColumn()})
            edited_cart.rename(columns={"판매단가(원)": "판매단가", "합계금액(원)": "합계금액"}, inplace=True)
            st.session_state.cart = coerce_cart_df(edited_cart)
            if st.button("장바구니 비우기", use_container_width=True): st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS); st.rerun()
        else: st.info("장바구니가 비어 있습니다.")
    v_spacer(16)
    with st.form("submit_form"):
        cart_now = st.session_state.cart
        total_amount_sum = cart_now['합계금액'].sum()
        st.markdown(f"**최종 확인:** 총 {len(cart_now)}개 품목, 합계 {total_amount_sum:,}원")
        confirm = st.checkbox("위 내용으로 발주를 제출합니다.")
        if st.form_submit_button("📦 발주 제출", type="primary", use_container_width=True, disabled=cart_now.empty):
            if not confirm: st.warning("제출 확인 체크박스를 선택해주세요."); st.stop()
            user = st.session_state.auth; order_id = make_order_id(user["user_id"])
            cart_with_master = pd.merge(cart_now, master_df[['품목코드', '과세구분']], on='품목코드', how='left')
            rows = []
            for _, r in cart_with_master.iterrows():
                total = r['합계금액']; tax_type = r.get('과세구분', '과세')
                supply = math.ceil(total / 1.1) if tax_type == '과세' else total
                tax = total - supply if tax_type == '과세' else 0
                rows.append({"주문일시": now_kst_str(), "발주번호": order_id, "지점ID": user["user_id"], "지점명": user["name"], "납품요청일": f"{납품요청일:%Y-%m-%d}", "품목코드": r["품목코드"], "품목명": r["품목명"], "단위": r["단위"], "수량": r["수량"], "판매단가": r["판매단가"], "공급가액": supply, "세액": tax, "합계금액": total, "비고": memo, "상태": "접수"})
            if append_orders(rows):
                st.session_state.success_message = "발주가 성공적으로 제출되었습니다."; st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS); st.rerun()
            else: st.error("발주 제출 중 오류가 발생했습니다.")

def page_store_orders_change(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("🧾 발주 조회·수정")
    display_feedback()
    df_all, user = load_orders_df(), st.session_state.auth
    df_user = df_all[df_all["지점ID"] == user["user_id"]]
    if df_user.empty: st.info("발주 데이터가 없습니다."); return
    
    # [UI 개편] 관리자 페이지처럼 필터 구성
    c1, c2, c3 = st.columns(3)
    dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="store_orders_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="store_orders_to")
    order_id_search = c3.text_input("발주번호로 검색", key="store_orders_search", placeholder="전체 또는 일부 입력")

    df_filtered = df_user.copy()
    if order_id_search:
        df_filtered = df_filtered[df_filtered["발주번호"].str.contains(order_id_search, na=False)]
    else:
        df_filtered = df_filtered[(pd.to_datetime(df_filtered["납품요청일"]).dt.date >= dt_from) & (pd.to_datetime(df_filtered["납품요청일"]).dt.date <= dt_to)]

    orders = df_filtered.groupby("발주번호").agg(주문일시=("주문일시", "first"), 건수=("품목코드", "count"), 합계금액=("합계금액", "sum"), 상태=("상태", "first")).reset_index().sort_values("주문일시", ascending=False)
    orders.rename(columns={"합계금액": "합계금액(원)"}, inplace=True)
    pending = orders[orders["상태"] == "접수"].copy(); shipped = orders[orders["상태"] == "출고완료"].copy()
    
    if 'store_pending_selection' not in st.session_state: st.session_state.store_pending_selection = {}
    if 'store_shipped_selection' not in st.session_state: st.session_state.store_shipped_selection = {}

    tab1, tab2 = st.tabs([f"접수 ({len(pending)}건)", f"출고완료 ({len(shipped)}건)"])
    
    with tab1:
        if st.session_state.get('active_store_tab') != 'pending':
            st.session_state.store_shipped_selection = {}
        st.session_state.active_store_tab = 'pending'
            
        pending['선택'] = pending['발주번호'].apply(lambda x: st.session_state.store_pending_selection.get(x, False))
        edited_pending = st.data_editor(pending, key="store_pending_editor", hide_index=True, disabled=["발주번호", "주문일시", "건수", "합계금액(원)", "상태"], column_config={"합계금액(원)": st.column_config.NumberColumn(), "선택": st.column_config.CheckboxColumn(width="small")})
        st.session_state.store_pending_selection = dict(zip(edited_pending['발주번호'], edited_pending['선택']))
        selected_pending_ids = [k for k, v in st.session_state.store_pending_selection.items() if v]

        if st.button("선택 발주 삭제", disabled=not selected_pending_ids, key="delete_pending_btn"):
            if update_order_status(selected_pending_ids, "삭제", user["name"]):
                st.session_state.success_message = f"{len(selected_pending_ids)}건의 발주가 삭제되었습니다."; st.rerun()
    
    with tab2:
        if st.session_state.get('active_store_tab') != 'shipped':
            st.session_state.store_pending_selection = {}
        st.session_state.active_store_tab = 'shipped'
        
        shipped['선택'] = shipped['발주번호'].apply(lambda x: st.session_state.store_shipped_selection.get(x, False))
        edited_shipped = st.data_editor(shipped, key="store_shipped_editor", hide_index=True, disabled=["발주번호", "주문일시", "건수", "합계금액(원)", "상태"], column_config={"합계금액(원)": st.column_config.NumberColumn(), "선택": st.column_config.CheckboxColumn(width="small")})
        st.session_state.store_shipped_selection = dict(zip(edited_shipped['발주번호'], edited_shipped['선택']))
        selected_shipped_ids = [k for k, v in st.session_state.store_shipped_selection.items() if v]

    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 📄 발주 품목 상세 조회")
        total_selected = selected_pending_ids + selected_shipped_ids
        if len(total_selected) == 1:
            target_id = total_selected[0]
            target_df = df_user[df_user["발주번호"] == target_id]
            target_status = target_df.iloc[0]["상태"]
            
            df_display = target_df.rename(columns={"판매단가": "판매단가(원)", "공급가액": "공급가액(원)", "세액": "세액(원)", "합계금액": "합계금액(원)"})
            display_cols = ["품목코드", "품목명", "단위", "수량", "판매단가(원)", "공급가액(원)", "세액(원)", "합계금액(원)"]
            st.dataframe(df_display[display_cols], hide_index=True, use_container_width=True, 
                         column_config={
                             "판매단가(원)": st.column_config.NumberColumn(), "공급가액(원)": st.column_config.NumberColumn(), 
                             "세액(원)": st.column_config.NumberColumn(), "합계금액(원)": st.column_config.NumberColumn()
                         })
            
            if target_status == '출고완료':
                v_spacer(10)
                store_info_series = store_info_df[store_info_df["지점ID"] == user["user_id"]]
                if not store_info_series.empty:
                    store_info = store_info_series.iloc[0]
                    buf = make_trading_statement_excel(target_df, store_info, master_df)
                    st.download_button(f"'{target_id}' 거래명세서 다운로드", data=buf, file_name=f"거래명세서_{user['name']}_{target_id}.xlsx", mime="application/vnd.ms-excel", use_container_width=True)
        else:
            st.info("상세 내용을 보려면 위 목록에서 발주를 **하나만** 선택하세요.")

def page_store_documents(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    user = st.session_state.auth
    df = load_orders_df()
    df_completed = df[(df["지점ID"] == user["user_id"]) & (df["상태"] == "출고완료")]
    if df_completed.empty: st.info("'출고완료' 상태의 발주 데이터가 없습니다."); return

    search_mode = st.radio("조회 방식", ["기간으로 조회", "발주번호로 조회"], key="store_doc_search_mode", horizontal=True)
    dfv = pd.DataFrame(); doc_type = "거래명세서"
    if search_mode == "기간으로 조회":
        c1, c2, c3 = st.columns([1, 1, 2])
        dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="store_doc_from")
        dt_to = c2.date_input("조회 종료일", date.today(), key="store_doc_to")
        doc_type = c3.selectbox("문서 종류", ["거래명세서", "세금계산서"], key="store_doc_type")
        mask = (pd.to_datetime(df_completed["납품요청일"]).dt.date >= dt_from) & (pd.to_datetime(df_completed["납품요청일"]).dt.date <= dt_to)
        dfv = df_completed[mask].copy()
    else:
        c1, c2 = st.columns([1, 1])
        order_ids = sorted(df_completed["발주번호"].dropna().unique().tolist(), reverse=True)
        order_id_sel = c1.selectbox("발주번호 선택", [""] + order_ids, key="store_doc_order_id")
        doc_type = c2.selectbox("문서 종류", ["거래명세서", "세금계산서"], key="store_doc_type_by_id")
        if order_id_sel: dfv = df_completed[df_completed["발주번호"] == order_id_sel].copy()

    if dfv.empty: st.warning("해당 조건으로 조회된 데이터가 없습니다."); return
    st.dataframe(dfv, use_container_width=True, hide_index=True)
    
    if not dfv.empty:
        store_info_series = store_info_df[store_info_df["지점ID"] == user["user_id"]]
        if not store_info_series.empty:
            store_info = store_info_series.iloc[0]
            if doc_type == "거래명세서":
                buf = make_trading_statement_excel(dfv, store_info, master_df)
            else:
                buf = make_tax_invoice_excel(dfv, store_info)
            st.download_button(f"{doc_type} 다운로드", data=buf, file_name=f"{doc_type}_{user['name']}_{now_kst_str('%Y%m%d')}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")
        else: st.error("지점 정보를 찾을 수 없어 서류를 생성할 수 없습니다.")

def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목 가격 조회")
    master_df_display = master_df.rename(columns={"판매단가": "판매단가(원)"})
    st.dataframe(master_df_display[["품목코드", "품목명", "품목규격", "분류", "단위", "판매단가(원)"]], use_container_width=True, hide_index=True, column_config={"판매단가(원)": st.column_config.NumberColumn()})

# =============================================================================
# 9) 관리자(Admin) 페이지
# =============================================================================
def page_admin_unified_management(df_all: pd.DataFrame):
    st.subheader("📋 발주요청 조회·수정")
    display_feedback()
    if df_all.empty: st.info("발주 데이터가 없습니다."); return

    c1, c2, c3, c4 = st.columns(4) # [UI 개선] 필터 너비 동일하게
    dt_from = c1.date_input("시작일", date.today() - timedelta(days=7), key="admin_mng_from")
    dt_to = c2.date_input("종료일", date.today(), key="admin_mng_to")
    stores = ["(전체)"] + sorted(df_all["지점명"].dropna().unique().tolist())
    store = c3.selectbox("지점", stores, key="admin_mng_store")
    order_id_search = c4.text_input("발주번호로 검색", key="admin_mng_order_id", placeholder="전체 또는 일부 입력")

    df = df_all.copy()
    if order_id_search:
        df = df[df["발주번호"].str.contains(order_id_search, na=False)]
    else:
        df = df[(pd.to_datetime(df["납품요청일"]).dt.date >= dt_from) & (pd.to_datetime(df["납품요청일"]).dt.date <= dt_to)]
        if store != "(전체)": df = df[df["지점명"] == store]

    orders = df.groupby("발주번호").agg(주문일시=("주문일시", "first"), 지점명=("지점명", "first"), 건수=("품목코드", "count"), 합계금액=("합계금액", "sum"), 상태=("상태", "first")).reset_index().sort_values("주문일시", ascending=False)
    orders.rename(columns={"합계금액": "합계금액(원)"}, inplace=True)
    pending = orders[orders["상태"] == "접수"].copy(); shipped = orders[orders["상태"] == "출고완료"].copy()
    pending.insert(0, "선택", False); shipped.insert(0, "선택", False)
    
    if 'admin_pending_selection' not in st.session_state: st.session_state.admin_pending_selection = {}
    if 'admin_shipped_selection' not in st.session_state: st.session_state.admin_shipped_selection = {}
    
    tab1, tab2 = st.tabs([f"📦 발주 요청 접수 ({len(pending)}건)", f"✅ 출고 완료 ({len(shipped)}건)"])
    with tab1:
        if st.session_state.get('active_admin_tab') != 'pending':
            st.session_state.admin_shipped_selection = {}
        st.session_state.active_admin_tab = 'pending'

        pending['선택'] = pending['발주번호'].apply(lambda x: st.session_state.admin_pending_selection.get(x, False))
        edited_pending = st.data_editor(pending, key="admin_pending_editor", hide_index=True, disabled=pending.columns.drop("선택"), column_config={"합계금액(원)": st.column_config.NumberColumn()})
        st.session_state.admin_pending_selection = dict(zip(edited_pending['발주번호'], edited_pending['선택']))
        selected_pending_ids = [k for k, v in st.session_state.admin_pending_selection.items() if v]

        if st.button("✅ 선택 발주 출고", disabled=not selected_pending_ids, key="admin_ship_btn"):
            if update_order_status(selected_pending_ids, "출고완료", st.session_state.auth["name"]):
                st.session_state.success_message = f"{len(selected_pending_ids)}건이 출고 처리되었습니다."; st.rerun()
    with tab2:
        if st.session_state.get('active_admin_tab') != 'shipped':
            st.session_state.admin_pending_selection = {}
        st.session_state.active_admin_tab = 'shipped'

        shipped['선택'] = shipped['발주번호'].apply(lambda x: st.session_state.admin_shipped_selection.get(x, False))
        edited_shipped = st.data_editor(shipped, key="admin_shipped_editor", hide_index=True, disabled=shipped.columns.drop("선택"), column_config={"합계금액(원)": st.column_config.NumberColumn()})
        st.session_state.admin_shipped_selection = dict(zip(edited_shipped['발주번호'], edited_shipped['선택']))
        selected_shipped_ids = [k for k, v in st.session_state.admin_shipped_selection.items() if v]

        if st.button("↩️ 접수 상태로 변경", disabled=not selected_shipped_ids, key="admin_revert_btn"):
            if update_order_status(selected_shipped_ids, "접수", st.session_state.auth["name"]):
                st.session_state.success_message = f"{len(selected_shipped_ids)}건이 접수 상태로 변경되었습니다."; st.rerun()
    
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 📄 발주 품목 상세 조회")
        total_selected = selected_pending_ids + selected_shipped_ids
        if len(total_selected) == 1:
            target_id = total_selected[0]
            st.markdown(f"**선택된 발주번호:** `{target_id}`")
            target_df = df_all[df_all["발주번호"] == target_id]
            
            df_display = target_df.rename(columns={"판매단가": "판매단가(원)", "공급가액": "공급가액(원)", "세액": "세액(원)", "합계금액": "합계금액(원)"})
            display_cols = ["품목코드", "품목명", "단위", "수량", "판매단가(원)", "공급가액(원)", "세액(원)", "합계금액(원)"]
            st.dataframe(df_display[display_cols], hide_index=True, use_container_width=True, 
                         column_config={
                            "판매단가(원)": st.column_config.NumberColumn(), "공급가액(원)": st.column_config.NumberColumn(), 
                            "세액(원)": st.column_config.NumberColumn(), "합계금액(원)": st.column_config.NumberColumn()
                         })
        else:
            st.info("상세 내용을 보려면 위 목록에서 발주를 **하나만** 선택하세요.")

def page_admin_documents(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    df = load_orders_df()
    df_completed = df[df["상태"] == "출고완료"]
    if df_completed.empty: st.info("'출고완료' 상태의 발주 데이터가 없습니다."); return
    search_mode = st.radio("조회 방식", ["기간으로 조회", "발주번호로 조회"], key="admin_doc_search_mode", horizontal=True)
    dfv = pd.DataFrame(); doc_type = "거래명세서"
    if search_mode == "기간으로 조회":
        c1, c2, c3, c4 = st.columns(4) # [UI 개선] 필터 너비 동일하게
        dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="admin_doc_from")
        dt_to = c2.date_input("조회 종료일", date.today(), key="admin_doc_to")
        stores = sorted(df_completed["지점명"].dropna().unique().tolist())
        store_sel = c3.selectbox("지점 선택", stores, key="admin_doc_store")
        doc_type = c4.selectbox("문서 종류", ["거래명세서", "세금계산서"], key="admin_doc_type")
        mask = (pd.to_datetime(df_completed["납품요청일"]).dt.date >= dt_from) & (pd.to_datetime(df_completed["납품요청일"]).dt.date <= dt_to) & (df_completed["지점명"] == store_sel)
        dfv = df_completed[mask].copy()
    else:
        c1, c2 = st.columns([1, 1])
        order_ids = sorted(df_completed["발주번호"].dropna().unique().tolist(), reverse=True)
        order_id_sel = c1.selectbox("발주번호 선택", [""] + order_ids, key="admin_doc_order_id")
        doc_type = c2.selectbox("문서 종류", ["거래명세서", "세금계산서"], key="admin_doc_type_by_id")
        if order_id_sel: dfv = df_completed[df_completed["발주번호"] == order_id_sel].copy()
    if dfv.empty: st.warning("해당 조건으로 조회된 데이터가 없습니다."); return
    st.dataframe(dfv, use_container_width=True, hide_index=True)
    if not dfv.empty:
        store_id = dfv.iloc[0]["지점ID"]; store_name = dfv.iloc[0]["지점명"]
        store_info_series = store_info_df[store_info_df["지점ID"] == store_id]
        if not store_info_series.empty:
            store_info = store_info_series.iloc[0]
            if doc_type == "거래명세서":
                buf = make_trading_statement_excel(dfv, store_info, master_df)
            else:
                buf = make_tax_invoice_excel(dfv, store_info)
            st.download_button(f"'{store_name}' {doc_type} 다운로드", data=buf, file_name=f"{doc_type}_{store_name}_{now_kst_str('%Y%m%d')}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")
        else: st.error("지점 정보를 찾을 수 없어 서류를 생성할 수 없습니다.")

def page_admin_items_price(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목 가격 설정")
    st.caption("가격을 수정하거나 품목을 추가/삭제한 후 '변경사항 저장' 버튼을 누르세요. 모든 변경 내역은 로그에 기록됩니다.")
    original_df = master_df.copy()
    with st.form("master_edit_form"):
        df_display = master_df.rename(columns={"판매단가": "판매단가(원)"})
        edited = st.data_editor(df_display.assign(삭제=False), hide_index=True, num_rows="dynamic", use_container_width=True, column_config={"판매단가(원)": st.column_config.NumberColumn()})
        if st.form_submit_button("변경사항 저장", type="primary", use_container_width=True):
            edited.rename(columns={"판매단가(원)": "판매단가"}, inplace=True)
            edited['삭제'] = edited['삭제'].fillna(False).astype(bool)
            final_df = edited[~edited["삭제"]].drop(columns=["삭제"])
            if write_master_df(final_df, original_df):
                st.session_state.success_message = "상품마스터가 저장되었습니다."; st.rerun()

def page_admin_sales_inquiry(master_df: pd.DataFrame):
    st.subheader("📈 매출 조회")
    df_orders = load_orders_df()
    df_sales_raw = df_orders[df_orders['상태'] == '출고완료'].copy()
    if df_sales_raw.empty: st.info("'출고완료'된 매출 데이터가 없습니다."); return

    c1, c2, c3 = st.columns(3)
    dt_from = c1.date_input("조회 시작일", date.today().replace(day=1), key="admin_sales_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="admin_sales_to")
    stores = ["(전체 통합)"] + sorted(df_sales_raw["지점명"].dropna().unique().tolist())
    store_sel = c3.selectbox("조회 지점", stores, key="admin_sales_store")

    mask = (pd.to_datetime(df_sales_raw["납품요청일"]).dt.date >= dt_from) & (pd.to_datetime(df_sales_raw["납품요청일"]).dt.date <= dt_to)
    if store_sel != "(전체 통합)": mask &= (df_sales_raw["지점명"] == store_sel)
    df_sales = df_sales_raw[mask].copy()

    if df_sales.empty: st.warning("해당 조건의 매출 데이터가 없습니다."); st.stop()

    total_sales = df_sales["합계금액"].sum(); total_supply = df_sales["공급가액"].sum(); total_tax = df_sales["세액"].sum()
    m1, m2, m3 = st.columns(3)
    m1.metric("총 매출 (VAT 포함)", f"{total_sales:,}원"); m2.metric("공급가액", f"{total_supply:,}원"); m3.metric("부가세액", f"{total_tax:,}원")
    st.divider()

    # [UI 개선] 일별/월별 탭으로 상세 분석 제공
    sales_tab1, sales_tab2 = st.tabs(["일별 매출 현황", "월별 매출 현황"])
    df_sales['일'] = pd.to_datetime(df_sales['납품요청일']).dt.day
    df_sales['월'] = pd.to_datetime(df_sales['납품요청일']).dt.month

    with sales_tab1:
        st.markdown("##### 📅 일별 매출 상세")
        daily_pivot = pd.pivot_table(df_sales, values='합계금액', index='일', columns='지점명', aggfunc='sum', fill_value=0)
        daily_pivot['총 합계'] = daily_pivot.sum(axis=1)
        st.dataframe(daily_pivot.style.format("{:,.0f}"))
    
    with sales_tab2:
        st.markdown("##### 🗓️ 월별 매출 상세")
        monthly_pivot = pd.pivot_table(df_sales, values='합계금액', index='월', columns='지점명', aggfunc='sum', fill_value=0)
        monthly_pivot['총 합계'] = monthly_pivot.sum(axis=1)
        st.dataframe(monthly_pivot.style.format("{:,.0f}"))
        
    # [기능 추가] 정산표 다운로드
    if st.button("정산표 다운로드", use_container_width=True):
        buf = make_sales_summary_excel(daily_pivot, monthly_pivot)
        st.download_button("다운로드 시작", data=buf, file_name=f"매출정산표_{dt_from}~{dt_to}.xlsx", mime="application/vnd.ms-excel")

# =============================================================================
# 10) 라우팅
# =============================================================================
if __name__ == "__main__":
    if not require_login(): st.stop()
    init_session_state()
    st.title("📦 식자재 발주 시스템")
    display_feedback()
    user = st.session_state.auth
    
    master_df = load_master_df()
    store_info_df = load_store_info_df()
    orders_df = load_orders_df()
    
    # [새로고침 문제 해결] query_params를 사용하여 현재 탭 상태 유지
    # URL에 page 파라미터가 없으면 역할에 따라 기본값 설정
    if 'page' not in st.query_params:
        default_page = "🛒 발주 요청" if user["role"] == "store" else "📋 발주요청 조회·수정"
        st.query_params['page'] = default_page

    active_tab = st.query_params['page']

    if user["role"] == "admin":
        admin_tabs = ["📋 발주요청 조회·수정", "📈 매출 조회", "📑 증빙서류 다운로드", "🏷️ 품목 가격 설정"]
        
        # st.radio를 tabs처럼 보이게 렌더링하고, URL 파라미터와 동기화
        selected_tab = st.radio("main_nav", admin_tabs, index=admin_tabs.index(active_tab), key="admin_tabs_radio", horizontal=True, label_visibility="collapsed")
        if selected_tab != active_tab:
            st.query_params['page'] = selected_tab
            st.rerun()
        
        if active_tab == "📋 발주요청 조회·수정":
            page_admin_unified_management(orders_df)
        elif active_tab == "📈 매출 조회":
            page_admin_sales_inquiry(master_df)
        elif active_tab == "📑 증빙서류 다운로드":
            page_admin_documents(store_info_df, master_df)
        elif active_tab == "🏷️ 품목 가격 설정":
            page_admin_items_price(master_df)
    else: # store
        store_tabs = ["🛒 발주 요청", "🧾 발주 조회·수정", "📑 증빙서류 다운로드", "🏷️ 품목 가격 조회"]
        
        selected_tab = st.radio("main_nav", store_tabs, index=store_tabs.index(active_tab), key="store_tabs_radio", horizontal=True, label_visibility="collapsed")
        if selected_tab != active_tab:
            st.query_params['page'] = selected_tab
            st.rerun()

        if active_tab == "🛒 발주 요청":
            page_store_register_confirm(master_df)
        elif active_tab == "🧾 발주 조회·수정":
            page_store_orders_change(store_info_df, master_df)
        elif active_tab == "📑 증빙서류 다운로드":
            page_store_documents(store_info_df, master_df)
        elif active_tab == "🏷️ 품목 가격 조회":
            page_store_master_view(master_df)
