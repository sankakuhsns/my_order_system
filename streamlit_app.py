# -*- coding: utf-8 -*-
# =============================================================================
# Streamlit 식자재 발주 시스템 (Cloud Secrets 전용 · 탭 네비게이션)
# - 발주자(지점): 발주 등록·확인 / 발주 조회·변경 / 발주서 조회·다운로드 / 발주 품목 가격 조회(조회 전용)
# - 관리자: 주문관리·출고 / 출고 조회·변경 / 납품내역서 / 납품 품목 및 가격(편집 저장)
# - 저장: Google Sheets (Streamlit Cloud Secrets 필수, 로컬 백업/게스트 진입 없음)
# =============================================================================

from io import BytesIO
from datetime import datetime, date, timedelta
from typing import Dict, Any, List
import pandas as pd
import streamlit as st

# ---- Google Sheets
import gspread
from google.oauth2 import service_account

# =============================================================================
# 0) 페이지/테마
# =============================================================================
st.set_page_config(page_title="발주 시스템", page_icon="📦", layout="wide")

THEME = {
    "BORDER": "#e8e8e8",
    "CARD": "background-color:#ffffff;border:1px solid #e8e8e8;border-radius:12px;padding:16px;",
    "PRIMARY": "#1C6758",
}
st.markdown(f"""
<style>
.small {{font-size: 12px; color: #777;}}
.card {{ {THEME["CARD"]} }}
.sticky-bottom {{
    position: sticky; bottom: 0; z-index: 999; {THEME["CARD"]} margin-top: 8px;
    display: flex; align-items:center; justify-content: space-between; gap: 16px;
}}
.metric {{font-weight:700; color:{THEME["PRIMARY"]};}}
</style>
""", unsafe_allow_html=True)

# =============================================================================
# 1) Secrets: users (로그인) 선검증
# 요구 스키마(권장): [users] 아래 JSON 중첩
# [users]
# jeondae = { password="jd", name="전대점", role="store" }
# hq      = { password="dj", name="대전공장", role="admin" }
# =============================================================================
def load_users_from_secrets() -> Dict[str, Dict[str, str]]:
    users = st.secrets.get("users", None)
    if not isinstance(users, dict) or len(users) == 0:
        st.error("로그인 계정이 없습니다. Streamlit Cloud → Settings → Secrets 의 [users] 섹션을 등록하세요.")
        st.stop()

    cleaned: Dict[str, Dict[str, str]] = {}
    for uid, payload in users.items():
        if not isinstance(payload, dict):
            st.error(f"[users.{uid}] 값이 객체(dict)가 아닙니다. 예: users.{uid} = {{ password=\"..\", name=\"..\", role=\"store\" }}")
            st.stop()
        pwd = str(payload.get("password", "")).strip()
        name = str(payload.get("name", uid)).strip()
        role = str(payload.get("role", "store")).strip().lower()
        if not pwd:
            st.error(f"[users.{uid}].password 가 비어있습니다."); st.stop()
        if role not in {"store", "admin"}:
            st.error(f"[users.{uid}].role 은 'store' 또는 'admin' 이어야 합니다. (현재: {role})"); st.stop()
        cleaned[str(uid)] = {"password": pwd, "name": name, "role": role}
    return cleaned

USERS = load_users_from_secrets()

# =============================================================================
# 2) 상수/컬럼
# =============================================================================
SHEET_NAME_MASTER = "상품마스터"
SHEET_NAME_ORDERS = "발주"
ORDER_STATUSES = ["접수", "출고완료"]
ORDERS_COLUMNS = ["주문일시","발주번호","지점ID","지점명","납품요청일",
                  "품목코드","품목명","단위","수량","비고","상태","처리일시","처리자"]

# =============================================================================
# 3) Google Sheets (실제 접근 시에만 검증)
# =============================================================================
def _require_google_secrets():
    google = st.secrets.get("google", {})
    required = ["type","project_id","private_key_id","private_key","client_email","client_id","SPREADSHEET_KEY"]
    missing = [k for k in required if not str(google.get(k, "")).strip()]
    if missing:
        st.error("Google 연동 설정이 부족합니다. Streamlit Cloud → Settings → Secrets 의 [google] 섹션을 확인하세요.")
        st.write("누락 항목:", ", ".join(missing))
        st.stop()
    return google

@st.cache_resource(show_spinner=False)
def get_gs_client():
    google = _require_google_secrets()
    creds = service_account.Credentials.from_service_account_info(
        google, scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    )
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def open_spreadsheet():
    google = _require_google_secrets()
    key = str(google["SPREADSHEET_KEY"]).strip()
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
    # 활성 필터(있을 때만)
    if "활성" in df.columns:
        mask = df["활성"].astype(str).str.lower().isin(["1","true","y","yes"])
        df = df[mask | df["활성"].isna()]
    return df

def write_master_df(df: pd.DataFrame) -> bool:
    cols = [c for c in ["품목코드","품목명","분류","단위","단가","활성"] if c in df.columns]
    df = df[cols].copy()
    try:
        sh = open_spreadsheet()
        try:
            ws = sh.worksheet(SHEET_NAME_MASTER)
        except Exception:
            ws = sh.add_worksheet(title=SHEET_NAME_MASTER, rows=2000, cols=25)
        ws.clear()
        values = [cols] + df.fillna("").values.tolist()
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
        return pd.DataFrame(ws.get_all_records())
    except Exception:
        return pd.DataFrame(columns=ORDERS_COLUMNS)

def write_orders_df(df: pd.DataFrame) -> bool:
    df = df[ORDERS_COLUMNS].copy()
    try:
        sh = open_spreadsheet()
        try:
            ws = sh.worksheet(SHEET_NAME_ORDERS)
        except Exception:
            ws = sh.add_worksheet(title=SHEET_NAME_ORDERS, rows=5000, cols=25)
        ws.clear()
        values = [ORDERS_COLUMNS] + df.fillna("").values.tolist()
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
# 5) 로그인
# =============================================================================
def require_login():
    st.session_state.setdefault("auth", {})
    if st.session_state["auth"].get("login", False):
        return True

    st.header("🔐 로그인")
    uid = st.text_input("아이디", key="login_uid")
    pwd = st.text_input("비밀번호", type="password", key="login_pw")

    if st.button("로그인", use_container_width=True):
        account = USERS.get(uid)
        if not account or str(pwd) != str(account["password"]):
            st.error("아이디 또는 비밀번호가 올바르지 않습니다.")
        else:
            st.session_state["auth"] = {
                "login": True, "user_id": uid, "name": account["name"], "role": account["role"]
            }
            st.success(f"{account['name']}님 환영합니다!")
            st.rerun()
    return False

# =============================================================================
# 6) 유틸
# =============================================================================
def make_order_id(store_id: str, seq: int) -> str:
    return f"{datetime.now():%Y%m%d-%H%M}-{store_id}-{seq:03d}"

def merge_price(df_orders: pd.DataFrame, master: pd.DataFrame) -> pd.DataFrame:
    if df_orders.empty: return df_orders.copy()
    price_map = master[["품목코드","단가"]].drop_duplicates()
    out = df_orders.merge(price_map, on="품목코드", how="left")
    out["수량"] = pd.to_numeric(out["수량"], errors="coerce").fillna(0).astype(int)
    out["단가"] = pd.to_numeric(out["단가"], errors="coerce").fillna(0).astype(int)
    out["금액"] = (out["수량"] * out["단가"]).astype(int)
    return out

def make_order_sheet_excel(df_note: pd.DataFrame, include_price: bool) -> BytesIO:
    """발주서/납품내역서 공용 엑셀 생성"""
    buf = BytesIO()
    cols = ["발주번호","주문일시","납품요청일","지점명","품목코드","품목명","단위","수량","비고","상태"]
    if include_price:
        for c in ["단가","금액"]:
            if c not in df_note.columns: df_note[c] = 0
        cols += ["단가","금액"]
    export = df_note[cols].copy().sort_values(["발주번호","품목코드"])
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        export.to_excel(w, index=False, sheet_name="발주내역")
        if include_price and "금액" in export.columns:
            ws = w.sheets["발주내역"]
            last = len(export) + 1
            ws.write(last, export.columns.get_loc("수량"), "총 수량")
            ws.write(last, export.columns.get_loc("수량")+1, int(export["수량"].sum()))
            ws.write(last, export.columns.get_loc("금액")-1, "총 금액")
            ws.write(last, export.columns.get_loc("금액"), int(export["금액"].sum()))
    buf.seek(0); return buf

# =============================================================================
# 7) 발주자(지점) 화면
# =============================================================================
def page_store_register_confirm(master_df: pd.DataFrame):
    st.subheader("🛒 발주 등록·확인")
    l, m, r = st.columns([1,1,2])
    with l:
        quick = st.radio("납품 선택", ["오늘","내일","직접선택"], horizontal=True, key="rq_radio")
    with m:
        납품요청일 = date.today() if quick=="오늘" else (date.today()+timedelta(days=1) if quick=="내일" else
                  st.date_input("납품 요청일", value=date.today(), key="rq_date"))
    with r:
        memo = st.text_input("요청 사항(선택)", key="rq_memo")

    c1, c2 = st.columns([2,1])
    with c1: keyword = st.text_input("품목 검색(이름/코드)", key="kw")
    with c2:
        if "분류" in master_df.columns:
            cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
            cat_sel = st.selectbox("분류(선택)", cat_opt, key="cat_sel")
        else:
            cat_sel = "(전체)"

    df_view = master_df.copy()
    if keyword:
        q = keyword.strip().lower()
        df_view = df_view[df_view.apply(lambda r: q in str(r.get("품목명","")).lower()
                                                  or q in str(r.get("품목코드","")).lower(), axis=1)]
    if "분류" in master_df.columns and cat_sel != "(전체)":
        df_view = df_view[df_view["분류"] == cat_sel]

    preview_cols = [c for c in ["품목코드","품목명","분류","단위"] if c in df_view.columns]
    st.dataframe(df_view[preview_cols].reset_index(drop=True), use_container_width=True, height=180)

    st.markdown("---")
    st.markdown("**발주 수량 입력** (수량만 수정)")
    edit_cols = [c for c in ["품목코드","품목명","단위"] if c in df_view.columns]
    df_edit = df_view[edit_cols].copy(); df_edit["수량"] = 0
    edited = st.data_editor(
        df_edit, disabled=edit_cols,
        column_config={"수량": st.column_config.NumberColumn(min_value=0, step=1)},
        use_container_width=True, num_rows="fixed", hide_index=True, height=360, key="order_editor_table"
    )

    sel_df = edited[edited["수량"].fillna(0).astype(float) > 0].copy()
    total_items = len(sel_df); total_qty = int(sel_df["수량"].sum()) if total_items>0 else 0
    st.markdown(f"""
    <div class="sticky-bottom">
        <div>납품 요청일: <b>{납품요청일.strftime('%Y-%m-%d')}</b></div>
        <div>선택 품목수: <span class="metric">{total_items:,}</span> 개</div>
        <div>총 수량: <span class="metric">{total_qty:,}</span></div>
    </div>
    """, unsafe_allow_html=True)

    confirm = st.checkbox("제출 전 입력 내용 확인했습니다.", value=False)
    if st.button("📦 발주 제출", type="primary", use_container_width=True):
        if sel_df.empty: st.warning("수량이 0보다 큰 품목이 없습니다."); st.stop()
        if not confirm: st.warning("체크박스를 확인해 주세요."); st.stop()
        user = st.session_state["auth"]; seq = st.session_state.get("order_seq", 1)
        order_id = make_order_id(user.get("user_id","STORE"), seq); st.session_state["order_seq"] = seq + 1
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for _, r in sel_df.iterrows():
            rows.append({
                "주문일시": now, "발주번호": order_id, "지점ID": user.get("user_id"), "지점명": user.get("name"),
                "납품요청일": str(납품요청일), "품목코드": r.get("품목코드"), "품목명": r.get("품목명"),
                "단위": r.get("단위"), "수량": int(r.get("수량",0) or 0), "비고": memo or "",
                "상태": "접수", "처리일시": "", "처리자": ""
            })
        ok = append_orders(rows)
        if ok: st.success(f"발주가 접수되었습니다. 발주번호: {order_id}")
        else: st.error("발주 저장에 실패했습니다.")

def page_store_orders_change():
    st.subheader("🧾 발주 조회·변경")
    df = load_orders_df().copy()
    user = st.session_state["auth"]
    if df.empty:
        st.info("발주 데이터가 없습니다."); return
    df = df[df["지점ID"].astype(str) == user.get("user_id")]
    c1, c2 = st.columns(2)
    with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=7))
    with c2: dt_to   = st.date_input("종료일", value=date.today())
    def _to_dt(s):
        try: return pd.to_datetime(s)
        except: return pd.NaT
    df["주문일시_dt"] = df["주문일시"].apply(_to_dt)
    mask = (df["주문일시_dt"].dt.date>=dt_from)&(df["주문일시_dt"].dt.date<=dt_to)
    dfv = df[mask].copy().sort_values(["주문일시","발주번호"])
    st.caption(f"조회 건수: {len(dfv):,}건")
    st.dataframe(dfv, use_container_width=True, height=360)

    st.markdown("---")
    st.markdown("**발주 변경 (출고 전 건만 수정 가능)**")
    editable = dfv[dfv["상태"]!="출고완료"].copy()
    if editable.empty:
        st.info("출고 전 상태의 발주가 없습니다."); return
    show_cols = ["발주번호","품목코드","품목명","단위","수량","비고"]
    edited = st.data_editor(
        editable[show_cols],
        column_config={"수량": st.column_config.NumberColumn(min_value=0, step=1)},
        use_container_width=True, num_rows="dynamic", hide_index=True, key="store_edit_orders"
    )
    if st.button("변경 내용 저장", type="primary"):
        base = df.copy()
        key_cols = ["발주번호","품목코드"]
        merged = base.merge(edited[key_cols+["수량","비고"]], on=key_cols, how="left", suffixes=("","_new"))
        base["수량"] = merged["수량_new"].combine_first(base["수량"])
        base["비고"] = merged["비고_new"].combine_first(base["비고"])
        ok = write_orders_df(base)
        if ok: st.success("변경사항을 저장했습니다."); st.rerun()
        else: st.error("저장 실패")

def page_store_order_form_download(master_df: pd.DataFrame):
    st.subheader("📑 발주서 조회·다운로드")
    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다."); return
    user = st.session_state["auth"]
    df = df[df["지점ID"].astype(str) == user.get("user_id")]
    c1, c2, c3 = st.columns([1,1,2])
    with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=7))
    with c2: dt_to   = st.date_input("종료일", value=date.today())
    with c3:
        order_ids = ["(전체)"] + sorted(df["발주번호"].dropna().unique().tolist())
        target_order = st.selectbox("발주번호(선택 시 해당 건만)", order_ids)
    def _to_dt(s):
        try: return pd.to_datetime(s)
        except: return pd.NaT
    df["주문일시_dt"] = df["주문일시"].apply(_to_dt)
    mask = (df["주문일시_dt"].dt.date>=dt_from)&(df["주문일시_dt"].dt.date<=dt_to)
    if target_order != "(전체)": mask &= (df["발주번호"]==target_order)
    dfv = df[mask].copy().sort_values(["발주번호","품목코드"])
    st.dataframe(dfv, use_container_width=True, height=420)
    # 발주서는 금액 없음
    buf = make_order_sheet_excel(dfv, include_price=False)
    st.download_button("발주서 엑셀 다운로드", data=buf.getvalue(),
                       file_name="발주서.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 발주 품목 가격 조회")
    cols = [c for c in ["품목코드","품목명","분류","단위","단가"] if c in master_df.columns]
    st.dataframe(master_df[cols], use_container_width=True, height=480)

# =============================================================================
# 8) 관리자 화면
# =============================================================================
def page_admin_orders_manage(master_df: pd.DataFrame):
    st.subheader("🗂️ 주문관리·출고")
    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다."); return
    c1, c2, c3, c4 = st.columns([1,1,1,2])
    with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=3))
    with c2: dt_to   = st.date_input("종료일", value=date.today())
    with c3:
        stores = ["(전체)"] + sorted(df["지점명"].dropna().unique().tolist())
        store = st.selectbox("지점", stores)
    with c4:
        status = st.multiselect("상태", ORDER_STATUSES, default=ORDER_STATUSES)
    def _to_dt(s):
        try: return pd.to_datetime(s)
        except: return pd.NaT
    df["주문일시_dt"] = df["주문일시"].apply(_to_dt)
    mask = (df["주문일시_dt"].dt.date>=dt_from)&(df["주문일시_dt"].dt.date<=dt_to)
    if store != "(전체)": mask &= (df["지점명"]==store)
    if status: mask &= df["상태"].isin(status)
    dfv = df[mask].copy().sort_values(["주문일시","발주번호"])
    dfv_price = merge_price(dfv, master_df)
    st.caption(f"조회 건수: {len(dfv):,}건")
    st.dataframe(dfv_price, use_container_width=True, height=420)
    st.download_button("CSV 다운로드", data=dfv_price.to_csv(index=False).encode("utf-8-sig"),
                       file_name="orders_admin.csv", mime="text/csv")
    st.markdown("---")
    st.markdown("**출고 처리 (이미 출고완료된 발주번호는 목록 제외)**")
    if not dfv.empty:
        candidates = sorted(dfv[dfv["상태"]=="접수"]["발주번호"].dropna().unique().tolist())
        sel_ids = st.multiselect("발주번호 선택", candidates, key="adm_pick_ids")
        if st.button("선택 발주 출고완료 처리", type="primary"):
            if sel_ids:
                ok = update_order_status(sel_ids, new_status="출고완료",
                                         handler=st.session_state["auth"].get("name","관리자"))
                if ok: st.success("출고완료 처리되었습니다."); st.rerun()
                else: st.error("상태 변경 실패")
            else:
                st.warning("발주번호를 선택하세요.")

def page_admin_shipments_change():
    st.subheader("🚚 출고 조회·변경")
    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다."); return
    c1, c2 = st.columns(2)
    with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=7))
    with c2: dt_to   = st.date_input("종료일", value=date.today())
    def _to_dt(s):
        try: return pd.to_datetime(s)
        except: return pd.NaT
    df["주문일시_dt"] = df["주문일시"].apply(_to_dt)
    mask = (df["주문일시_dt"].dt.date>=dt_from)&(df["주문일시_dt"].dt.date<=dt_to)
    dfv = df[mask].copy()
    st.caption(f"조회 건수: {len(dfv):,}건")
    st.dataframe(dfv.sort_values(["주문일시","발주번호"]), use_container_width=True, height=360)
    st.markdown("---")
    st.markdown("**출고 상태 일괄 변경**")
    order_ids = sorted(dfv["발주번호"].dropna().unique().tolist())
    target = st.multiselect("발주번호", order_ids, key="ship_change_ids")
    new_status = st.selectbox("새 상태", ORDER_STATUSES, index=0)
    if st.button("상태 변경 저장", type="primary"):
        if not target: st.warning("발주번호를 선택하세요."); return
        ok = update_order_status(target, new_status=new_status,
                                 handler=st.session_state["auth"].get("name","관리자"))
        if ok: st.success("상태 변경 완료"); st.rerun()
        else: st.error("상태 변경 실패")

def page_admin_delivery_note(master_df: pd.DataFrame):
    st.subheader("📑 납품내역서")
    df = load_orders_df().copy()
    if df.empty:
        st.info("발주 데이터가 없습니다."); return
    c1, c2, c3 = st.columns([1,1,2])
    with c1: dt_from = st.date_input("시작일", value=date.today()-timedelta(days=7))
    with c2: dt_to   = st.date_input("종료일", value=date.today())
    with c3:
        order_ids = ["(전체)"] + sorted(df["발주번호"].dropna().unique().tolist())
        target_order = st.selectbox("발주번호(선택 시 해당 건만)", order_ids)
    def _to_dt(s):
        try: return pd.to_datetime(s)
        except: return pd.NaT
    df["주문일시_dt"] = df["주문일시"].apply(_to_dt)
    mask = (df["주문일시_dt"].dt.date>=dt_from)&(df["주문일시_dt"].dt.date<=dt_to)
    if target_order != "(전체)": mask &= (df["발주번호"]==target_order)
    dfv = df[mask].copy().sort_values(["발주번호","품목코드"])
    df_note = merge_price(dfv, master_df)  # 관리자는 금액 포함
    st.dataframe(df_note, use_container_width=True, height=420)
    buf = make_order_sheet_excel(df_note, include_price=True)
    st.download_button("납품내역서 엑셀 다운로드", data=buf.getvalue(),
                       file_name="납품내역서.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

def page_admin_items_price(master_df: pd.DataFrame):
    st.subheader("🏷️ 납품 품목 및 가격 (시트 반영)")
    cols = [c for c in ["품목코드","품목명","분류","단위","단가","활성"] if c in master_df.columns]
    view = master_df[cols].copy()
    st.caption("단가·활성(선택)을 수정 후 [변경사항 저장]을 누르면 상품마스터 시트에 반영됩니다.")
    edited = st.data_editor(
        view, use_container_width=True, hide_index=True, num_rows="dynamic",
        column_config={"단가": st.column_config.NumberColumn(min_value=0, step=1),
                       "활성": st.column_config.CheckboxColumn()},
        key="master_editor"
    )
    if st.button("변경사항 저장", type="primary"):
        if "단가" in edited.columns:
            edited["단가"] = pd.to_numeric(edited["단가"], errors="coerce").fillna(0).astype(int)
        ok = write_master_df(edited)
        if ok:
            st.success("상품마스터에 저장되었습니다.")
            st.cache_data.clear(); st.rerun()
        else:
            st.error("저장 실패")

# =============================================================================
# 9) 라우팅 (탭 네비게이션)
# =============================================================================
if __name__ == "__main__":
    st.title("📦 식자재 발주 시스템")
    st.caption("Streamlit Cloud Secrets 전용 · 탭 기반 네비게이션")

    if not require_login():
        st.stop()

    user = st.session_state["auth"]
    role = user.get("role", "store")
    master = load_master_df()

    if role == "admin":
        t1, t2, t3, t4 = st.tabs(["주문관리·출고", "출고 조회·변경", "납품내역서", "납품 품목 및 가격"])
        with t1: page_admin_orders_manage(master)
        with t2: page_admin_shipments_change()
        with t3: page_admin_delivery_note(master)
        with t4: page_admin_items_price(master)
    else:
        t1, t2, t3, t4 = st.tabs(["발주 등록·확인", "발주 조회·변경", "발주서 조회·다운로드", "발주 품목 가격 조회"])
        with t1: page_store_register_confirm(master)
        with t2: page_store_orders_change()
        with t3: page_store_order_form_download(master)
        with t4: page_store_master_view(master)
