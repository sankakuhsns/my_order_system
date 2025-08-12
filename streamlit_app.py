# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (UI 리뉴얼 + 발주/출고서 포맷 + 삭제/수정 안정화)
# =============================================================================

from io import BytesIO
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
from collections.abc import Mapping

import hashlib
import pandas as pd
import streamlit as st

# Google Sheets
import gspread
from google.oauth2 import service_account

# Excel export
# (requirements: streamlit, pandas, gspread, google-auth, gspread-dataframe, xlsxwriter)
import xlsxwriter  # noqa: F401 (엔진 로딩용)

# -----------------------------------------------------------------------------
# 페이지/테마 (탭 간격/로그인 카드/버튼 강조 등)
# -----------------------------------------------------------------------------
st.set_page_config(page_title="발주 시스템", page_icon="📦", layout="wide")
THEME = {
    "BORDER": "#e8e8e8",
    "CARD": "background-color:#ffffff;border:1px solid #e8e8e8;border-radius:14px;padding:18px;",
    "PRIMARY": "#1C6758",
}

st.markdown(f"""
<style>
/***** 공통 카드/텍스트 *****/
.card {{ {THEME["CARD"]} }}
.small {{ font-size: 12px; color: #777; }}
.metric {{ font-weight:700; color:{THEME["PRIMARY"]}; }}

/***** 탭 가독성 향상 *****/
.stTabs [role="tablist"] {{
  gap: 12px !important;
  margin: 8px 0 18px !important;
}}
.stTabs [role="tab"] {{
  padding: 10px 16px !important;
  border: 1px solid #e8e8e8 !important;
  border-bottom: 2px solid transparent !important;
  border-radius: 10px 10px 0 0 !important;
}}

/***** 로그인 영역 *****/
.login-wrap {{ display:flex; justify-content:center; margin-top:4vh; }}
.login-card {{ width: 420px; max-width: 92vw; {THEME["CARD"]} box-shadow:0 6px 18px rgba(0,0,0,0.04); }}
.login-title {{ text-align:center; font-size: 36px; font-weight: 800; margin: 10px 0 24px; letter-spacing:-0.5px; }}
.login-sub {{ text-align:center; color:#666; margin-bottom: 10px; }}
.login-input input {{ width: 260px !important; }}

/***** 하단 고정 요약바 *****/
.sticky-bottom {{
  position: sticky; bottom: 0; z-index: 999; {THEME["CARD"]} margin-top: 8px;
  display:flex; align-items:center; justify-content: space-between; gap: 16px;
}}

/***** 표 높이 가독성 *****/
.dataframe th, .dataframe td {{ padding: 8px 6px; }}
</style>
""", unsafe_allow_html=True)

# =============================================================================
# 1) Users 로더 (여러 시크릿 포맷 지원)
# =============================================================================

def _normalize_account(uid: str, payload: Mapping) -> dict:
    pwd_plain = payload.get("password")
    pwd_hash  = payload.get("password_hash")
    name = str(payload.get("name", uid)).strip()
    role = str(payload.get("role", "store")).strip().lower()
    if not (pwd_plain or pwd_hash):
        st.error(f"[users.{uid}]에 password 또는 password_hash가 필요합니다."); st.stop()
    if role not in {"store", "admin"}:
        st.error(f"[users.{uid}].role 은 'store' 또는 'admin' 이어야 합니다. (현재: {role})"); st.stop()
    return {
        "password": (str(pwd_plain) if pwd_plain is not None else None),
        "password_hash": (str(pwd_hash).lower() if pwd_hash is not None else None),
        "name": name, "role": role,
    }


def load_users_from_secrets() -> Dict[str, Dict[str, str]]:
    cleaned: Dict[str, Dict[str, str]] = {}
    users_root = st.secrets.get("users", None)

    if isinstance(users_root, Mapping) and len(users_root) > 0:
        for uid, payload in users_root.items():
            if isinstance(payload, Mapping):
                cleaned[str(uid)] = _normalize_account(str(uid), payload)
    elif isinstance(users_root, list) and users_root:
        for row in users_root:
            if not isinstance(row, Mapping):
                continue
            uid = row.get("user_id") or row.get("uid") or row.get("id")
            if uid:
                cleaned[str(uid)] = _normalize_account(str(uid), row)

    if not cleaned:
        for uid in ("jeondae", "hq"):
            dotted_key = f"users.{uid}"
            payload = st.secrets.get(dotted_key, None)
            if isinstance(payload, Mapping):
                cleaned[str(uid)] = _normalize_account(str(uid), payload)
        if not cleaned:
            try:
                for k, v in dict(st.secrets).items():
                    if isinstance(k, str) and k.startswith("users.") and isinstance(v, Mapping):
                        uid = k.split(".", 1)[1].strip()
                        if uid:
                            cleaned[str(uid)] = _normalize_account(uid, v)
            except Exception:
                pass

    if not cleaned:
        with st.expander("🔍 Secrets 진단 (민감값 비노출)"):
            try:
                top_keys = list(dict(st.secrets).keys())
            except Exception:
                top_keys = []
            st.write({
                "has_users_section_as_mapping": isinstance(users_root, Mapping),
                "users_section_type": type(users_root).__name__,
                "top_level_keys": top_keys[:50],
            })
        st.error("로그인 계정을 찾을 수 없습니다. Secrets 의 [users.jeondae], [users.hq] 구조를 확인하세요.")
        st.stop()

    return cleaned


USERS = load_users_from_secrets()

# =============================================================================
# 2) 시트/스키마 정의
# =============================================================================
SHEET_NAME_MASTER = "상품마스터"      # 품목코드, 품목명, 분류, 단위, 단가, 활성
SHEET_NAME_ORDERS = "발주"             # 확정 스키마 (아래 ORDERS_COLUMNS 참고)
ORDER_STATUSES = ["접수", "출고완료"]
ORDERS_COLUMNS = [
    "주문일시","발주번호","지점ID","지점명","납품요청일",
    "품목코드","품목명","단위","수량","단가","금액",
    "비고","상태","처리일시","처리자"
]

# =============================================================================
# 3) Google Sheets 연결
# =============================================================================

def _require_google_secrets():
    google = st.secrets.get("google", {})
    required = ["type","project_id","private_key_id","private_key","client_email","client_id"]
    missing = [k for k in required if not str(google.get(k, "")).strip()]
    if missing:
        st.error("Google 연동 설정이 부족합니다. Secrets 의 [google] 섹션을 확인하세요.")
        st.write("누락 항목:", ", ".join(missing))
        st.stop()
    return google


@st.cache_resource(show_spinner=False)
def get_gs_client():
    google = _require_google_secrets()
    google = dict(google)
    pk = str(google.get("private_key", ""))
    if "\\n" in pk:
        google["private_key"] = pk.replace("\\n", "\n")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = service_account.Credentials.from_service_account_info(google, scopes=scopes)
    return gspread.authorize(creds)


@st.cache_resource(show_spinner=False)
def open_spreadsheet():
    g = st.secrets.get("google", {})
    key = str(g.get("SPREADSHEET_KEY") or st.secrets.get("SPREADSHEET_KEY", "")).strip()
    if not key:
        st.error("Secrets 에 SPREADSHEET_KEY가 없습니다. [google].SPREADSHEET_KEY 또는 루트 SPREADSHEET_KEY 설정 필요.")
        st.stop()
    try:
        return get_gs_client().open_by_key(key)
    except Exception as e:
        st.error(f"스프레드시트 열기 실패: {e}")
        st.stop()


# =============================================================================
# 4) 데이터 I/O
# =============================================================================

@st.cache_data(ttl=180)
def load_master_df() -> pd.DataFrame:
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_MASTER)
        df = pd.DataFrame(ws.get_all_records())
    except Exception:
        df = pd.DataFrame()
    if df.empty:
        df = pd.DataFrame([
            {"품목코드":"P001","품목명":"오이","단위":"EA","분류":"채소","단가":800,"활성":True},
            {"품목코드":"P002","품목명":"대파","단위":"KG","분류":"채소","단가":15600,"활성":True},
            {"품목코드":"P003","품목명":"간장","단위":"L","분류":"조미료","단가":3500,"활성":True},
        ])
    for c in ["품목코드","품목명","단위","분류","단가","활성"]:
        if c not in df.columns:
            df[c] = (0 if c=="단가" else (True if c=="활성" else ""))
    # 활성 필터
    if "활성" in df.columns:
        mask = df["활성"].astype(str).str.lower().isin(["1","true","y","yes"])
        df = df[mask | df["활성"].isna()]
    # 정수 단가 보정
    df["단가"] = pd.to_numeric(df.get("단가", 0), errors="coerce").fillna(0).astype(int)
    return df


def write_master_df(df: pd.DataFrame) -> bool:
    cols = [c for c in ["품목코드","품목명","분류","단위","단가","활성"] if c in df.columns]
    out = df[cols].copy()
    try:
        sh = open_spreadsheet()
        try:
            ws = sh.worksheet(SHEET_NAME_MASTER)
        except Exception:
            ws = sh.add_worksheet(title=SHEET_NAME_MASTER, rows=2000, cols=25)
        ws.clear()
        values = [cols] + out.fillna("").values.tolist()
        ws.update("A1", values)
        load_master_df.clear()
        return True
    except Exception as e:
        st.error(f"상품마스터 저장 실패: {e}")
        return False


@st.cache_data(ttl=120)
def load_orders_df() -> pd.DataFrame:
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_ORDERS)
        df = pd.DataFrame(ws.get_all_records())
    except Exception:
        df = pd.DataFrame()
    for c in ORDERS_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    df = df[ORDERS_COLUMNS].copy()
    return df


def write_orders_df(df: pd.DataFrame) -> bool:
    out = df[ORDERS_COLUMNS].copy()
    try:
        sh = open_spreadsheet()
        try:
            ws = sh.worksheet(SHEET_NAME_ORDERS)
        except Exception:
            ws = sh.add_worksheet(title=SHEET_NAME_ORDERS, rows=5000, cols=25)
        ws.clear()
        values = [ORDERS_COLUMNS] + out.fillna("").values.tolist()
        ws.update("A1", values)
        load_orders_df.clear()
        return True
    except Exception as e:
        st.error(f"발주 저장 실패: {e}")
        return False


def append_orders(rows: List[Dict[str, Any]]) -> bool:
    base = load_orders_df()
    df_new = pd.DataFrame(rows)[ORDERS_COLUMNS]
    return write_orders_df(pd.concat([base, df_new], ignore_index=True))


def update_order_status(selected_ids: List[str], new_status: str, handler: str) -> bool:
    df = load_orders_df().copy()
    if df.empty:
        st.warning("변경할 데이터가 없습니다."); return False
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mask = df["발주번호"].astype(str).isin([str(x) for x in selected_ids])
    df.loc[mask, "상태"] = new_status
    df.loc[mask, "처리일시"] = now
    df.loc[mask, "처리자"] = handler
    return write_orders_df(df)


# =============================================================================
# 5) 로그인 (가운데 정렬 · 입력폭 축소 · 시인성 강화)
# =============================================================================

def verify_password(input_pw: str, stored_hash: Optional[str], fallback_plain: Optional[str]) -> bool:
    if stored_hash:
        h = stored_hash.strip().lower()
        if h.startswith("sha256$"):
            h = h.split("$", 1)[1].strip()
        digest = hashlib.sha256(input_pw.encode()).hexdigest()
        return digest == h
    if fallback_plain is not None:
        return str(input_pw) == str(fallback_plain)
    return False


def _find_account(uid_or_name: str):
    s = str(uid_or_name or "").strip()
    if not s:
        return None, None
    lower_map = {k.lower(): k for k in USERS.keys()}
    if s in USERS:
        return s, USERS[s]
    if s.lower() in lower_map:
        real_uid = lower_map[s.lower()]
        return real_uid, USERS[real_uid]
    for uid, acct in USERS.items():
        nm = str(acct.get("name", "")).strip()
        if s == nm or s.lower() == nm.lower():
            return uid, acct
    return None, None


def _do_login(uid_input: str, pwd: str) -> bool:
    real_uid, acct = _find_account(uid_input)
    if not acct:
        st.error("아이디(또는 지점명) 또는 비밀번호가 올바르지 않습니다.")
        return False
    ok = verify_password(input_pw=pwd, stored_hash=acct.get("password_hash"), fallback_plain=acct.get("password"))
    if not ok:
        st.error("아이디(또는 지점명) 또는 비밀번호가 올바르지 않습니다.")
        return False
    st.session_state["auth"] = {"login": True, "user_id": real_uid, "name": acct["name"], "role": acct["role"]}
    st.success(f"{acct['name']}님 환영합니다!")
    st.rerun()
    return True


def require_login() -> bool:
    st.session_state.setdefault("auth", {})
    if st.session_state["auth"].get("login", False):
        return True

    # 중앙 정렬 로그인 카드
    st.markdown("<div class='login-wrap'>", unsafe_allow_html=True)
    with st.container(border=False):
        st.markdown("<div class='login-card'>", unsafe_allow_html=True)
        st.markdown("<div class='login-title'>식자재 발주 시스템</div>", unsafe_allow_html=True)
        st.markdown("<div class='login-sub'>아이디 또는 지점명과 비밀번호를 입력하세요.</div>", unsafe_allow_html=True)
        with st.form("login_form", border=False):
            uid = st.text_input("아이디 또는 지점명", key="login_uid", help="예: jeondae 또는 전대점")
            pwd = st.text_input("비밀번호", type="password", key="login_pw")
            col1, col2, col3 = st.columns([1,2,1])
            with col2:
                submitted = st.form_submit_button("로그인", use_container_width=True)
            if submitted:
                _do_login(uid, pwd)
        st.markdown("</div>", unsafe_allow_html=True)  # login-card
    st.markdown("</div>", unsafe_allow_html=True)      # login-wrap
    return False


# =============================================================================
# 6) 유틸
# =============================================================================

def make_order_id(store_id: str, seq: int) -> str:
    return f"{datetime.now():%Y%m%d-%H%M}-{store_id}-{seq:03d}"


def _ensure_datetime_col(df: pd.DataFrame, src_col: str, dst_col: str = "주문일시_dt") -> pd.DataFrame:
    df[dst_col] = pd.to_datetime(df[src_col], errors="coerce", utc=False)
    return df


def _range_filename(prefix: str, dt_from: date, dt_to: date) -> str:
    return f"{prefix} {dt_from:%y%m%d}~{dt_to:%y%m%d}.xlsx"


def make_order_sheet_excel(df_note: pd.DataFrame, include_price: bool, title: str, period_text: str) -> BytesIO:
    """발주/출고 내역 엑셀 생성 (헤더 타이틀/기간/합계 포함; NaN 안전)"""
    buf = BytesIO()

    # 내보낼 컬럼 구성
    cols = ["발주번호","주문일시","납품요청일","지점명","품목코드","품목명","단위","수량","비고","상태"]
    if include_price:
        for c in ["단가","금액"]:
            if c not in df_note.columns:
                df_note[c] = 0
        cols += ["단가","금액"]

    export = df_note[cols].copy().sort_values(["발주번호","품목코드"]).reset_index(drop=True)

    # 숫자형 보정
    export["수량"] = pd.to_numeric(export.get("수량", 0), errors="coerce").fillna(0)
    if include_price:
        export["금액"] = pd.to_numeric(export.get("금액", 0), errors="coerce").fillna(0)
        export["단가"] = pd.to_numeric(export.get("단가", 0), errors="coerce").fillna(0)

    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        # 표는 4행 아래부터 시작하여 상단에 제목/기간/생성일 배치
        startrow = 4
        export.to_excel(w, index=False, sheet_name="내역", startrow=startrow)
        ws = w.sheets["내역"]
        wb = w.book

        ncols = len(export.columns)
        fmt_title = wb.add_format({"bold": True, "font_size": 18, "align": "center"})
        fmt_sub = wb.add_format({"font_size": 11, "align": "center", "color": "#555555"})
        fmt_sum_label = wb.add_format({"bold": True})
        fmt_int = wb.add_format({"num_format": "#,##0"})

        # 머지 타이틀/기간
        ws.merge_range(0, 0, 0, ncols-1, title, fmt_title)
        ws.merge_range(1, 0, 1, ncols-1, period_text, fmt_sub)
        ws.merge_range(2, 0, 2, ncols-1, f"생성일시: {datetime.now():%Y-%m-%d %H:%M}", fmt_sub)

        # 합계 행 (표 아래)
        last = startrow + len(export) + 1  # header 한 줄 포함
        # 수량 합계
        sum_qty = int(round(export["수량"].sum()))
        ws.write(last, export.columns.get_loc("수량"), "총 수량", fmt_sum_label)
        ws.write(last, export.columns.get_loc("수량") + 1, sum_qty, fmt_int)
        # 금액 합계
        if include_price:
            sum_amt = int(round(export["금액"].sum()))
            ws.write(last, export.columns.get_loc("금액") - 1, "총 금액", fmt_sum_label)
            ws.write(last, export.columns.get_loc("금액"), sum_amt, fmt_int)

        # 숫자열 서식 적용
        ws.set_column(export.columns.get_loc("수량"), export.columns.get_loc("수량"), 10, fmt_int)
        if include_price:
            ws.set_column(export.columns.get_loc("단가"), export.columns.get_loc("단가"), 12, fmt_int)
            ws.set_column(export.columns.get_loc("금액"), export.columns.get_loc("금액"), 14, fmt_int)

    buf.seek(0)
    return buf


# =============================================================================
# 7) 발주(지점) 화면
# =============================================================================

def page_store_register_confirm(master_df: pd.DataFrame):
    st.subheader("🛒 발주 등록 · 확인")

    # ── 상단 옵션
    l, m, r = st.columns([1,1,2])
    with l:
        quick = st.radio("납품 선택", ["오늘","내일","직접선택"], horizontal=True, key="store_quick_radio")
    with m:
        납품요청일 = (
            date.today() if quick=="오늘" else
            (date.today()+timedelta(days=1) if quick=="내일" else
             st.date_input("납품 요청일", value=date.today(), key="store_req_date"))
        )
    with r:
        memo = st.text_input("요청 사항(선택)", key="store_req_memo")

    # ── 검색/필터
    c1, c2 = st.columns([2,1])
    with c1:
        keyword = st.text_input("품목 검색(이름/코드)", key="store_kw")
    with c2:
        if "분류" in master_df.columns:
            cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
            cat_sel = st.selectbox("분류(선택)", cat_opt, key="store_cat_sel")
        else:
            cat_sel = "(전체)"

    df_view = master_df.copy()
    if keyword:
        q = keyword.strip().lower()
        df_view = df_view[df_view.apply(lambda r: q in str(r.get("품목명","")) .lower() or q in str(r.get("품목코드","")) .lower(), axis=1)]
    if "분류" in master_df.columns and cat_sel != "(전체)":
        df_view = df_view[df_view["분류"] == cat_sel]

    preview_cols = [c for c in ["품목코드","품목명","분류","단위","단가"] if c in df_view.columns]
    st.dataframe(df_view[preview_cols].reset_index(drop=True), use_container_width=True, height=320)

    # ── 수량 입력 (단가 표시 + 금액 미리보기)
    st.markdown("---")
    st.markdown("**발주 수량 입력** (수량만 수정, 단가/금액 자동 계산)")

    edit_cols = [c for c in ["품목코드","품목명","단위","단가"] if c in df_view.columns]
    df_edit = df_view[edit_cols].copy(); df_edit["수량"] = 0

    edited = st.data_editor(
        df_edit,
        disabled=[c for c in edit_cols],
        column_config={
            "수량": st.column_config.NumberColumn(min_value=0, step=1, help="키보드 ↑/↓ 또는 숫자 입력")
        },
        use_container_width=True, num_rows="fixed", hide_index=True, height=420, key="store_order_editor"
    )

    # 선택 건 요약 (금액 포함)
    sel_df = edited[edited["수량"].fillna(0).astype(float) > 0].copy()
    if not sel_df.empty:
        sel_df["금액"] = (pd.to_numeric(sel_df.get("수량",0), errors="coerce").fillna(0) * pd.to_numeric(sel_df.get("단가",0), errors="coerce").fillna(0)).astype(int)
        st.dataframe(sel_df[["품목코드","품목명","단위","단가","수량","금액"]].reset_index(drop=True), use_container_width=True, height=260)

    total_items = len(sel_df)
    total_qty = int(sel_df["수량"].sum()) if total_items>0 else 0
    total_amt = int(sel_df["금액"].sum()) if total_items>0 else 0

    st.markdown(f"""
    <div class="sticky-bottom">
      <div>납품 요청일: <b>{납품요청일.strftime('%Y-%m-%d')}</b></div>
      <div>선택 품목수: <span class="metric">{total_items:,}</span> 개</div>
      <div>총 수량: <span class="metric">{total_qty:,}</span></div>
      <div>총 금액: <span class="metric">{total_amt:,}</span> 원</div>
    </div>
    """, unsafe_allow_html=True)

    confirm = st.checkbox("제출 전 입력 내용 확인했습니다.", value=False, key="store_confirm_chk")
    if st.button("📦 발주 제출", type="primary", use_container_width=True, key="store_submit_btn"):
        if sel_df.empty:
            st.warning("수량이 0보다 큰 품목이 없습니다."); st.stop()
        if not confirm:
            st.warning("체크박스를 확인해 주세요."); st.stop()

        # 단가 스냅샷으로 금액 저장
        user = st.session_state["auth"]
        seq = st.session_state.get("order_seq", 1)
        order_id = make_order_id(user.get("user_id","STORE"), seq)
        st.session_state["order_seq"] = seq + 1
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        rows = []
        for _, r in sel_df.iterrows():
            code = r.get("품목코드")
            unit = r.get("단위")
            qty  = int(r.get("수량",0) or 0)
            unit_price = int(pd.to_numeric(r.get("단가", 0), errors="coerce"))
            amount = int(qty * unit_price)
            rows.append({
                "주문일시": now, "발주번호": order_id, "지점ID": user.get("user_id"), "지점명": user.get("name"),
                "납품요청일": str(납품요청일), "품목코드": code, "품목명": r.get("품목명"),
                "단위": unit, "수량": qty, "단가": unit_price, "금액": amount,
                "비고": memo or "", "상태": "접수", "처리일시": "", "처리자": ""
            })
        ok = append_orders(rows)
        if ok: st.success(f"발주가 접수되었습니다. 발주번호: {order_id}")
        else: st.error("발주 저장에 실패했습니다.")



def page_store_orders_change():
    st.subheader("🧾 발주 조회 · 변경")
    df = load_orders_df().copy()
    user = st.session_state["auth"]
    if df.empty:
        st.info("발주 데이터가 없습니다."); return

    df = df[df["지점ID"].astype(str) == user.get("user_id")]

    c1, c2 = st.columns(2)
    with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=7), key="store_edit_from")
    with c2: dt_to   = st.date_input("종료일", value=date.today(), key="store_edit_to")

    df = _ensure_datetime_col(df, "주문일시")
    mask = df["주문일시_dt"].notna() & (df["주문일시_dt"].dt.date >= dt_from) & (df["주문일시_dt"].dt.date <= dt_to)
    dfv = df[mask].copy().sort_values(["주문일시_dt","발주번호"])  # 조회용

    orders = sorted(dfv["발주번호"].dropna().unique().tolist())
    if not orders:
        st.info("해당 기간에 발주가 없습니다."); return

    sel_order = st.selectbox("발주번호 선택 (눌러서 내용 확인/수정)", orders, key="store_edit_pick_order")

    # 선택 발주 상세
    target = dfv[dfv["발주번호"] == sel_order].copy()
    shipped = (target["상태"].astype(str) == "출고완료").any()

    st.caption(f"선택 발주 건수: {len(target):,}건  · 상태: {'출고완료' if shipped else '접수'}")
    st.dataframe(target.sort_values(["품목코드"]), use_container_width=True, height=220)

    if shipped:
        st.info("이미 출고완료된 발주로 수정할 수 없습니다.")
        return

    # 편집 테이블 (삭제 체크 포함)
    show_cols = ["품목코드","품목명","단위","수량","단가","비고"]
    editable = target[show_cols].copy()
    editable["삭제"] = False

    edited = st.data_editor(
        editable,
        column_config={
            "수량": st.column_config.NumberColumn(min_value=0, step=1),
            "단가": st.column_config.NumberColumn(min_value=0, step=1),
            "삭제": st.column_config.CheckboxColumn(help="체크 시 해당 품목을 발주에서 삭제")
        },
        use_container_width=True, hide_index=True, height=360, key="store_edit_orders_editor"
    )

    colA, colB = st.columns([1,1])
    with colA:
        do_del_zero = st.checkbox("수량=0 인 행 자동 삭제", value=True, key="store_edit_auto_drop")
    with colB:
        st.write("")

    if st.button("변경 내용 저장", type="primary", key="store_edit_save"):
        base = load_orders_df().copy()
        other_mask = base["발주번호"] != sel_order

        keep = edited.copy()
        keep["수량"] = pd.to_numeric(keep.get("수량", 0), errors="coerce").fillna(0).astype(int)
        keep["단가"] = pd.to_numeric(keep.get("단가", 0), errors="coerce").fillna(0).astype(int)
        if do_del_zero:
            keep = keep[keep["수량"] > 0]
        keep = keep[keep["삭제"] == False].drop(columns=["삭제"])  # noqa: E712
        if keep.empty:
            st.warning("모든 품목을 삭제할 수는 없습니다.")
            return
        keep["금액"] = (keep["수량"] * keep["단가"]).astype(int)

        # 선택 주문의 메타 정보 유지
        sample = target.iloc[0]
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for _, r in keep.iterrows():
            rows.append({
                "주문일시": sample["주문일시"],
                "발주번호": sel_order,
                "지점ID": sample["지점ID"],
                "지점명": sample["지점명"],
                "납품요청일": sample["납품요청일"],
                "품목코드": r["품목코드"],
                "품목명": r["품목명"],
                "단위": r["단위"],
                "수량": int(r["수량"]),
                "단가": int(r["단가"]),
                "금액": int(r["금액"]),
                "비고": r.get("비고", ""),
                "상태": "접수",
                "처리일시": now,
                "처리자": st.session_state["auth"].get("name", "")
            })
        new_df = pd.DataFrame(rows)[ORDERS_COLUMNS]

        out = pd.concat([base[other_mask], new_df], ignore_index=True)
        ok = write_orders_df(out)
        if ok:
            st.success("변경사항을 저장했습니다.")
            st.rerun()
        else:
            st.error("저장 실패")



def page_store_order_form_download(master_df: pd.DataFrame):
    st.subheader("📑 발주서 조회 · 다운로드")
    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다."); return
    user = st.session_state["auth"]
    df = df[df["지점ID"].astype(str) == user.get("user_id")]

    c1, c2, c3 = st.columns([1,1,2])
    with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=7), key="store_dl_from")
    with c2: dt_to   = st.date_input("종료일", value=date.today(), key="store_dl_to")
    with c3:
        order_ids = ["(전체)"] + sorted(df["발주번호"].dropna().unique().tolist())
        target_order = st.selectbox("발주번호(선택 시 해당 건만)", order_ids, key="store_dl_orderid")

    df = _ensure_datetime_col(df, "주문일시")
    mask = df["주문일시_dt"].notna() & (df["주문일시_dt"].dt.date >= dt_from) & (df["주문일시_dt"].dt.date <= dt_to)
    if target_order != "(전체)":
        mask &= (df["발주번호"]==target_order)
    dfv = df[mask].copy().sort_values(["발주번호","품목코드"])  
    st.dataframe(dfv, use_container_width=True, height=420)

    period_text = f"조회기간: {dt_from:%Y-%m-%d} ~ {dt_to:%Y-%m-%d}"
    buf = make_order_sheet_excel(dfv, include_price=False, title="산카쿠 납품내역서", period_text=period_text)
    fname = _range_filename("산카쿠 납품내역서", dt_from, dt_to)
    st.download_button("발주서 엑셀 다운로드", data=buf.getvalue(), file_name=fname, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="store_dl_btn")



def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 발주 품목 가격 조회")
    cols = [c for c in ["품목코드","품목명","분류","단위","단가"] if c in master_df.columns]
    st.dataframe(master_df[cols], use_container_width=True, height=480)


# =============================================================================
# 8) 관리자 화면
# =============================================================================

def page_admin_orders_manage(master_df: pd.DataFrame):
    st.subheader("🗂️ 주문 관리 · 출고 확인")
    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다."); return

    c1, c2, c3, c4 = st.columns([1,1,1,2])
    with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=3), key="admin_mng_from")
    with c2: dt_to   = st.date_input("종료일", value=date.today(), key="admin_mng_to")
    with c3:
        stores = ["(전체)"] + sorted(df["지점명"].dropna().unique().tolist())
        store = st.selectbox("지점", stores, key="admin_mng_store")
    with c4:
        status = st.multiselect("상태", ORDER_STATUSES, default=ORDER_STATUSES, key="admin_mng_status")

    df = _ensure_datetime_col(df, "주문일시")
    mask = df["주문일시_dt"].notna() & (df["주문일시_dt"].dt.date >= dt_from) & (df["주문일시_dt"].dt.date <= dt_to)
    if store != "(전체)": mask &= (df["지점명"]==store)
    if status: mask &= df["상태"].isin(status)
    dfv = df[mask].copy().sort_values(["주문일시_dt","발주번호"])

    st.caption(f"조회 건수: {len(dfv):,}건")
    st.dataframe(dfv, use_container_width=True, height=420)
    st.download_button("CSV 다운로드", data=dfv.to_csv(index=False).encode("utf-8-sig"), file_name="orders_admin.csv", mime="text/csv", key="admin_mng_csv")

    st.markdown("---")
    st.markdown("**출고 처리 (이미 출고완료된 발주번호는 목록 제외)**")
    if not dfv.empty:
        candidates = sorted(dfv[dfv["상태"]=="접수"]["발주번호"].dropna().unique().tolist())
        sel_ids = st.multiselect("발주번호 선택", candidates, key="admin_mng_pick_ids")
        if st.button("선택 발주 출고완료 처리", type="primary", key="admin_mng_ship_btn"):
            if sel_ids:
                ok = update_order_status(sel_ids, new_status="출고완료", handler=st.session_state["auth"].get("name","관리자"))
                if ok: st.success("출고완료 처리되었습니다."); st.rerun()
                else: st.error("상태 변경 실패")
            else:
                st.warning("발주번호를 선택하세요.")



def page_admin_shipments_change():
    st.subheader("🚚 출고내역 조회 · 상태변경")
    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다."); return

    c1, c2 = st.columns(2)
    with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=7), key="admin_ship_from")
    with c2: dt_to   = st.date_input("종료일", value=date.today(), key="admin_ship_to")

    df = _ensure_datetime_col(df, "주문일시")
    mask = df["주문일시_dt"].notna() & (df["주문일시_dt"].dt.date >= dt_from) & (df["주문일시_dt"].dt.date <= dt_to)
    dfv = df[mask].copy()
    st.caption(f"조회 건수: {len(dfv):,}건")
    st.dataframe(dfv.sort_values(["주문일시_dt","발주번호"]), use_container_width=True, height=360)

    st.markdown("---")
    st.markdown("**출고 상태 일괄 변경**")
    order_ids = sorted(dfv["발주번호"].dropna().unique().tolist())
    target = st.multiselect("발주번호", order_ids, key="admin_ship_change_ids")
    new_status = st.selectbox("새 상태", ORDER_STATUSES, index=0, key="admin_ship_new_status")
    if st.button("상태 변경 저장", type="primary", key="admin_ship_save"):
        if not target: st.warning("발주번호를 선택하세요."); return
        ok = update_order_status(target, new_status=new_status, handler=st.session_state["auth"].get("name","관리자"))
        if ok: st.success("상태 변경 완료"); st.rerun()
        else: st.error("상태 변경 실패")



def page_admin_delivery_note(master_df: pd.DataFrame):
    st.subheader("📑 출고 내역서 조회 · 다운로드")
    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다."); return

    c1, c2, c3 = st.columns([1,1,2])
    with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=7), key="admin_note_from")
    with c2: dt_to   = st.date_input("종료일", value=date.today(), key="admin_note_to")
    with c3:
        order_ids = ["(전체)"] + sorted(df["발주번호"].dropna().unique().tolist())
        target_order = st.selectbox("발주번호(선택 시 해당 건만)", order_ids, key="admin_note_orderid")

    df = _ensure_datetime_col(df, "주문일시")
    mask = df["주문일시_dt"].notna() & (df["주문일시_dt"].dt.date >= dt_from) & (df["주문일시_dt"].dt.date <= dt_to)
    if target_order != "(전체)":
        mask &= (df["발주번호"]==target_order)
    dfv = df[mask].copy().sort_values(["발주번호","품목코드"])  

    st.dataframe(dfv, use_container_width=True, height=420)

    period_text = f"조회기간: {dt_from:%Y-%m-%d} ~ {dt_to:%Y-%m-%d}"
    buf = make_order_sheet_excel(dfv, include_price=True, title="산카쿠 납품내역서", period_text=period_text)
    fname = _range_filename("산카쿠 납품내역서", dt_from, dt_to)
    st.download_button("출고 내역서 엑셀 다운로드", data=buf.getvalue(), file_name=fname, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="admin_note_btn")



def page_admin_items_price(master_df: pd.DataFrame):
    st.subheader("🏷️ 납품 품목 가격 설정")
    cols = [c for c in ["품목코드","품목명","분류","단위","단가","활성"] if c in master_df.columns]
    view = master_df[cols].copy()
    view["삭제"] = False
    st.caption("단가·활성 수정 후 저장하세요. [삭제] 체크 시 해당 행은 마스터에서 제거됩니다.")

    edited = st.data_editor(
        view,
        use_container_width=True, hide_index=True, num_rows="dynamic",
        column_config={
            "단가": st.column_config.NumberColumn(min_value=0, step=1),
            "활성": st.column_config.CheckboxColumn(),
            "삭제": st.column_config.CheckboxColumn()
        },
        key="admin_master_editor"
    )

    if st.button("변경사항 저장", type="primary", key="admin_master_save"):
        out = edited.copy()
        out = out[out["삭제"] == False].drop(columns=["삭제"])  # noqa: E712
        if "단가" in out.columns:
            out["단가"] = pd.to_numeric(out["단가"], errors="coerce").fillna(0).astype(int)
        ok = write_master_df(out)
        if ok:
            st.success("상품마스터에 저장되었습니다.")
            st.cache_data.clear(); st.rerun()
        else:
            st.error("저장 실패")


# =============================================================================
# 9) 라우팅
# =============================================================================
if __name__ == "__main__":
    # 제목(크게) — 로그인 화면에서는 카드 내부에 제목을 별도로 렌더링
    st.markdown("""
    <div style='text-align:center; margin-bottom: 6px;'>
      <span style='font-size:40px; font-weight:800; letter-spacing:-0.8px;'>식자재 발주 시스템</span>
    </div>
    """, unsafe_allow_html=True)

    if not require_login():
        st.stop()

    user = st.session_state["auth"]
    role = user.get("role", "store")
    master = load_master_df()

    if role == "admin":
        t1, t2, t3, t4 = st.tabs(["주문 관리·출고확인", "출고내역 조회·상태변경", "출고 내역서 다운로드", "납품 품목 가격 설정"])
        with t1: page_admin_orders_manage(master)
        with t2: page_admin_shipments_change()
        with t3: page_admin_delivery_note(master)
        with t4: page_admin_items_price(master)
    else:
        t1, t2, t3, t4 = st.tabs(["발주 등록·확인", "발주 조회·변경", "발주서 다운로드", "발주 품목 가격 조회"])
        with t1: page_store_register_confirm(master)
        with t2: page_store_orders_change()
        with t3: page_store_order_form_download(master)
        with t4: page_store_master_view(master)
