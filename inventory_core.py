from __future__ import annotations

import datetime as dt
import os
import sqlite3
from dataclasses import dataclass
from typing import Optional

import pandas as pd


DB_PATH = os.path.join(os.path.dirname(__file__), "inventory.db")


@dataclass(frozen=True)
class CoreConfig:
    default_lead_time_days: int = 7
    default_safety_stock: int = 0


CFG = CoreConfig()

# 대시보드 load_latest 전용 (SELECT * 대신 명시 컬럼으로 I/O·메모리 절약)
SNAPSHOT_SELECT_SQL = """
SELECT snapshot_date, sku, name, category, stock, channel_stock, warehouse_stock,
       warehouse1_stock, warehouse2_stock, min_stock, lead_time_days, safety_stock,
       sales_qty, updated_at, distribution_note,
       COALESCE(assort_ratio, 0) AS assort_ratio,
       COALESCE(assort_box_count, 0) AS assort_box_count
FROM snapshots WHERE snapshot_date = ?
"""


def init_db(db_path: str = DB_PATH) -> None:
    # [최적화] 스키마·마이그레이션을 앱 기동 시 1회만 실행
    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                snapshot_date TEXT NOT NULL,
                sku TEXT NOT NULL,
                name TEXT,
                category TEXT,
                stock INTEGER NOT NULL,
                channel_stock INTEGER,
                warehouse_stock INTEGER,
                warehouse1_stock INTEGER,
                warehouse2_stock INTEGER,
                warehouse1_solid INTEGER DEFAULT 0,
                warehouse1_assort INTEGER DEFAULT 0,
                warehouse2_solid INTEGER DEFAULT 0,
                warehouse2_assort INTEGER DEFAULT 0,
                assort_ratio INTEGER DEFAULT 0,
                assort_box_count INTEGER DEFAULT 0,
                min_stock INTEGER,
                lead_time_days INTEGER,
                safety_stock INTEGER,
                sales_qty INTEGER,
                updated_at TEXT,
                PRIMARY KEY (snapshot_date, sku)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_snapshots_sku_date ON snapshots (sku, snapshot_date);"
        )

        try:
            cursor = conn.execute("PRAGMA table_info(snapshots)")
            columns = [row[1] for row in cursor.fetchall()]
            if "sales_qty" not in columns:
                conn.execute("ALTER TABLE snapshots ADD COLUMN sales_qty INTEGER DEFAULT 0")
                conn.commit()
            if "channel_stock" not in columns:
                conn.execute("ALTER TABLE snapshots ADD COLUMN channel_stock INTEGER DEFAULT 0")
                conn.commit()
            if "warehouse_stock" not in columns:
                conn.execute("ALTER TABLE snapshots ADD COLUMN warehouse_stock INTEGER DEFAULT 0")
                conn.commit()
            if "warehouse1_stock" not in columns:
                conn.execute("ALTER TABLE snapshots ADD COLUMN warehouse1_stock INTEGER DEFAULT 0")
                conn.commit()
            if "warehouse2_stock" not in columns:
                conn.execute("ALTER TABLE snapshots ADD COLUMN warehouse2_stock INTEGER DEFAULT 0")
                conn.commit()
            if "distribution_note" not in columns:
                conn.execute("ALTER TABLE snapshots ADD COLUMN distribution_note TEXT")
                conn.commit()
        except Exception:
            pass

        for col in (
            "warehouse1_solid",
            "warehouse1_assort",
            "warehouse2_solid",
            "warehouse2_assort",
        ):
            try:
                conn.execute(f"ALTER TABLE snapshots ADD COLUMN {col} INTEGER DEFAULT 0")
            except Exception:
                pass
        conn.commit()

        for col in ("assort_ratio", "assort_box_count"):
            try:
                conn.execute(f"ALTER TABLE snapshots ADD COLUMN {col} INTEGER DEFAULT 0")
            except Exception:
                pass
        conn.commit()

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS omni_blocked (
                snapshot_date TEXT NOT NULL,
                style_code TEXT NOT NULL,
                sku_code TEXT NOT NULL,
                blocked_qty INTEGER NOT NULL,
                top_store TEXT,
                PRIMARY KEY (snapshot_date, style_code, sku_code)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    # [최적화] 연결만 생성; 스키마는 init_db에서 처리
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def normalize_excel(df: pd.DataFrame, snapshot_date: dt.date, return_failed: bool = False):
    colmap = {
        "sku": "sku",
        "SKU": "sku",
        "자체 품목코드": "sku",  # 이전 형식 지원
        "상품 품목코드": "product_item_code",  # 새 형식: 임시 저장
        "품목코드(15자리)": "sku",  # 구글시트 재고판매현황 등: 완성 SKU
        "상품": "sku_raw",  # 엑셀2 F열: SKU (15-16자리)
        "상품코드": "sku",  # 매장재고 엑셀: 상품코드
        "name": "name",
        "상품명": "name",
        "품목명": "name",
        "category": "category",
        "카테고리": "category",
        "분류": "category",
        "옵션": "option",  # 새 형식: 컬러/사이즈 추출용
        "stock": "stock",
        "재고": "stock",
        "현재재고": "stock",
        "재고수량": "stock",
        "판매수량": "sales_qty",  # 새 형식: 7일 판매량
        "기간내 판매수량": "sales_qty",  # API/시트: 조회기간 합계 판매
        "결제수량": "order_qty",
        "환불수량": "refund_qty",
        "가용재고": "channel_stock",  # 매장재고 엑셀: 가용재고
        "매장재고": "channel_stock",
        "솔리드가용재고": "solid_stock",  # 물류창고: 솔리드
        "아소트가용재고": "assort_stock",  # 물류창고: 아소트
        "min_stock": "min_stock",
        "최소재고": "min_stock",
        "안전재고": "safety_stock",
        "safety_stock": "safety_stock",
        "lead_time_days": "lead_time_days",
        "리드타임": "lead_time_days",
        "리드타임(일)": "lead_time_days",
    }

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    original_columns = df.columns.tolist()  # 원본 컬럼 저장
    rename = {c: colmap[c] for c in df.columns if c in colmap}
    df = df.rename(columns=rename)

    # 중복 컬럼 처리: 같은 이름의 컬럼이 여러 개면 첫 번째만 사용
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated(keep='first')]

    # 엑셀 2 형식: F열 SKU를 15자리로 변환
    if "sku_raw" in df.columns and "sku" not in df.columns:
        df["sku"] = df["sku_raw"].astype(str).str.strip().str[:15]
    
    # 새 형식: SKU 생성 (상품코드 10자리 + E열 컬러 + E열 사이즈)
    # 이미 파일에 완성 sku(예: 품목코드(15자리))가 있으면 덮어쓰지 않음 — 옵션 형식이
    # "Color : … / Size : …"가 아닌 "(10)WHITE / S(085)" 등인 시트는 extract_sku가 실패함
    if "option" in df.columns and "sku" not in df.columns:
        import re
        
        def extract_sku(row):
            # 브랜드 SKU는 무조건 S로 시작해야 함
            base_code = ""
            
            # 우선순위 1: 상품 품목코드(product_item_code)에서 S로 시작하는 코드 찾기
            if "product_item_code" in row and pd.notna(row.get("product_item_code")):
                product_code = str(row.get("product_item_code", "")).strip()
                # S로 시작하고 10자리 이상이면 사용
                if product_code.startswith('S') and len(product_code) >= 10:
                    base_code = product_code[:10]
            
            # 우선순위 2: 상품명(D열)에서 SP로 시작하는 코드 찾기
            if not base_code and "name" in row:
                name_str = str(row.get("name", ""))
                import re
                
                # 방법 1: _(W), _(M) 접두사를 무시하고 SP 코드 찾기
                # 패턴: _ 또는 공백 다음에 선택적으로 (W)/(M), 그 다음 SP + 8자리
                sp_pattern = re.search(r'[_\s](?:\([WMwm]\))?(SP[A-Z0-9]{8})\b', name_str, re.IGNORECASE)
                
                if sp_pattern:
                    base_code = sp_pattern.group(1).upper()[:10]
                elif "_" in name_str:
                    # 방법 2: _ 로 분리하여 SP 코드 찾기 (백업 로직)
                    parts = name_str.split("_")
                    for part in reversed(parts):  # 뒤에서부터 찾기
                        part = part.strip()
                        # (W), (M) 접두사 제거
                        clean_part = re.sub(r'^\([WMwm]\)', '', part).strip()
                        # SP로 시작하고 10자리 이상이면 사용
                        if clean_part.upper().startswith('SP') and len(clean_part) >= 10:
                            base_code = clean_part[:10].upper()
                            break
                        # S로 시작하는 것도 지원 (하위 호환성, SP가 없을 때만)
                        elif not base_code and clean_part.upper().startswith('S') and len(clean_part) >= 10:
                            base_code = clean_part[:10].upper()
            
            # E열(옵션)에서 컬러코드와 사이즈코드 추출
            # 옵션은 줄바꿈(\n)으로 구분됨: "Color : (10)WHITE\nSize : M(095)"
            option_str = str(row.get("option", ""))
            
            color_code = ""
            size_code = ""
            
            # 줄바꿈 또는 공백으로 분리
            lines = option_str.replace('\n', ' ').replace('\r', ' ')
            
            # Color 부분에서 괄호 안의 코드 추출
            # 둥근 괄호: (10)WHITE → 10, (VI)VINTAGE → VI
            color_match = re.search(r'Color\s*:\s*\(([A-Z0-9]+)\)', lines, re.IGNORECASE)
            if color_match:
                color_code = color_match.group(1)
            else:
                # 대괄호: [PK]PALE PINK → PK
                color_match = re.search(r'Color\s*:\s*\[([A-Z0-9]+)\]', lines, re.IGNORECASE)
                if color_match:
                    color_code = color_match.group(1)
            
            # Size 부분에서 3자리 숫자 추출
            # 방법 1: 괄호 안의 숫자: M(095), 32(082) → 095, 082
            size_match = re.search(r'Size\s*:\s*[A-Z0-9]*\((\d{3})\)', lines, re.IGNORECASE)
            if size_match:
                size_code = size_match.group(1)
            else:
                # 방법 2: 괄호 없는 3자리 숫자: Size : 120 → 120
                size_match = re.search(r'Size\s*:\s*(\d{3})\b', lines, re.IGNORECASE)
                if size_match:
                    size_code = size_match.group(1)
                else:
                    # 방법 3: 프리 사이즈: FRE, FREE, 프리 → 000
                    if re.search(r'Size\s*:\s*(FRE|FREE|프리)', lines, re.IGNORECASE):
                        size_code = "000"
            
            # SKU 조합 (13~17자리 허용: 컬러/사이즈 코드 길이 가변)
            if base_code and color_code and size_code and len(base_code) == 10:
                generated_sku = base_code + color_code + size_code
                # S로 시작하고 13자리 이상이면 유효한 SKU로 인정
                if len(generated_sku) >= 13 and generated_sku.startswith('S'):
                    return generated_sku
            
            # 생성 실패시 None 반환 (나중에 필터링)
            return None
        
        df["sku"] = df.apply(extract_sku, axis=1)

    # SKU는 필수, 나머지는 선택 (물류재고 파일은 stock 컬럼 없음)
    required = ["sku"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"필수 컬럼이 없습니다: {missing}. (예: sku/상품코드)")

    for c in ["name", "category", "stock", "min_stock", "lead_time_days", "safety_stock", "sales_qty", "channel_stock", "solid_stock", "assort_stock"]:
        if c not in df.columns:
            df[c] = None

    # snapshot_date는 dt.date 타입이므로 .isoformat() 사용
    if isinstance(snapshot_date, dt.date):
        snap_str = snapshot_date.isoformat()
    else:
        snap_str = pd.Timestamp(snapshot_date).strftime("%Y-%m-%d")
    df["snapshot_date"] = snap_str
    
    # 실패한 행 추적용 (return_failed=True일 때만)
    if return_failed:
        original_df = df.copy()
        original_df["sku_original"] = original_df["sku"]
        original_df["실패사유"] = ""
    
    # sku 컬럼에서 NaN 제거 (SKU 생성 실패한 행)
    nan_mask = df["sku"].isna()
    if return_failed and nan_mask.any():
        original_df.loc[nan_mask, "실패사유"] = "SKU 생성 실패 (상품명 형식 오류)"
    df = df[df["sku"].notna()].copy()
    
    # sku 컬럼을 문자열로 변환하고 공백 제거
    df["sku"] = df["sku"].astype(str).str.strip()
    
    # 빈 문자열이 아닌 행만 필터링
    empty_mask = df["sku"] == ""
    if return_failed and empty_mask.any():
        original_df.loc[empty_mask.index[empty_mask], "실패사유"] = "빈 SKU"
    df = df[df["sku"] != ""].copy()
    
    none_mask = df["sku"] == "None"
    if return_failed and none_mask.any():
        original_df.loc[none_mask.index[none_mask], "실패사유"] = "None SKU"
    df = df[df["sku"] != "None"].copy()
    
    # SKU 길이 검증 (13자리 이상 허용)
    if "option" in df.columns or "상품" in original_columns:
        len_mask = df["sku"].str.len() < 13
        if return_failed and len_mask.any():
            original_df.loc[len_mask.index[len_mask], "실패사유"] = f"SKU 길이 오류 (13자리 미만)"
        df = df[df["sku"].str.len() >= 13].copy()

    df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
    df["min_stock"] = pd.to_numeric(df["min_stock"], errors="coerce")
    df["lead_time_days"] = pd.to_numeric(df["lead_time_days"], errors="coerce")
    df["safety_stock"] = pd.to_numeric(df["safety_stock"], errors="coerce")
    df["sales_qty"] = pd.to_numeric(df["sales_qty"], errors="coerce").fillna(0).astype(int)
    df["channel_stock"] = pd.to_numeric(df["channel_stock"], errors="coerce").fillna(0).astype(int)
    df["solid_stock"] = pd.to_numeric(df["solid_stock"], errors="coerce").fillna(0).astype(int)
    df["assort_stock"] = pd.to_numeric(df["assort_stock"], errors="coerce").fillna(0).astype(int)
    
    # 물류창고 재고 계산
    df["warehouse_stock"] = df["solid_stock"] + df["assort_stock"]

    df["lead_time_days"] = df["lead_time_days"].fillna(CFG.default_lead_time_days).astype(int)
    df["safety_stock"] = df["safety_stock"].fillna(CFG.default_safety_stock).astype(int)
    df["min_stock"] = df["min_stock"].fillna(df["safety_stock"]).fillna(0).astype(int)

    df["updated_at"] = dt.datetime.now().isoformat(timespec="seconds")
    
    # 물류센터별 재고 초기화
    if "warehouse1_stock" not in df.columns:
        df["warehouse1_stock"] = 0
    if "warehouse2_stock" not in df.columns:
        df["warehouse2_stock"] = 0
    df["warehouse1_stock"] = df["warehouse1_stock"].fillna(0).astype(int)
    df["warehouse2_stock"] = df["warehouse2_stock"].fillna(0).astype(int)

    keep = [
        "snapshot_date",
        "sku",
        "name",
        "category",
        "stock",
        "channel_stock",
        "warehouse_stock",
        "warehouse1_stock",
        "warehouse2_stock",
        "min_stock",
        "lead_time_days",
        "safety_stock",
        "sales_qty",
        "updated_at",
    ]
    success_df = df[keep].drop_duplicates(subset=["snapshot_date", "sku"], keep="last")
    
    # 실패한 행 반환 (요청 시)
    if return_failed:
        # 실패 사유가 있는 행만 추출
        failed_df = original_df[original_df["실패사유"] != ""].copy()
        return success_df, failed_df
    
    return success_df


def upsert_snapshot(conn: sqlite3.Connection, snap: pd.DataFrame) -> int:
    rows = snap.to_dict(orient="records")
    conn.executemany(
        """
        INSERT INTO snapshots (
            snapshot_date, sku, name, category, stock, channel_stock, warehouse_stock, warehouse1_stock, warehouse2_stock, 
            min_stock, lead_time_days, safety_stock, sales_qty, updated_at
        ) VALUES (
            :snapshot_date, :sku, :name, :category, :stock, :channel_stock, :warehouse_stock, :warehouse1_stock, :warehouse2_stock,
            :min_stock, :lead_time_days, :safety_stock, :sales_qty, :updated_at
        )
        ON CONFLICT(snapshot_date, sku) DO UPDATE SET
            name=excluded.name,
            category=excluded.category,
            stock=excluded.stock,
            channel_stock=excluded.channel_stock,
            warehouse_stock=excluded.warehouse_stock,
            warehouse1_stock=excluded.warehouse1_stock,
            warehouse2_stock=excluded.warehouse2_stock,
            min_stock=excluded.min_stock,
            lead_time_days=excluded.lead_time_days,
            safety_stock=excluded.safety_stock,
            sales_qty=excluded.sales_qty,
            updated_at=excluded.updated_at
        """,
        rows,
    )
    conn.commit()
    
    # 모든 스냅샷에 대해 warehouse_stock 재계산 (항만 + 부평)
    if rows:
        snapshot_date = rows[0].get("snapshot_date")
        conn.execute(
            """
            UPDATE snapshots
            SET warehouse_stock = COALESCE(warehouse1_stock, 0) + COALESCE(warehouse2_stock, 0)
            WHERE snapshot_date = ?
            """,
            (snapshot_date,)
        )
        conn.commit()
    
    return len(rows)


def update_channel_stock(conn: sqlite3.Connection, snapshot_date: str, sku_channel_map: dict) -> int:
    # [최적화] executemany + total_changes로 실제 변경 행 수 집계
    """매장재고 업데이트"""
    if not sku_channel_map:
        return 0
    now = dt.datetime.now().isoformat(timespec="seconds")
    rows = [
        (channel_stock, now, snapshot_date, sku)
        for sku, channel_stock in sku_channel_map.items()
    ]
    n0 = conn.total_changes
    conn.executemany(
        """
        UPDATE snapshots
        SET channel_stock = ?, updated_at = ?
        WHERE snapshot_date = ? AND sku = ?
        """,
        rows,
    )
    conn.commit()
    return conn.total_changes - n0


def update_distribution_note(conn: sqlite3.Connection, snapshot_date: str, sku_note_map: dict) -> int:
    # [최적화] executemany + total_changes로 실제 변경 행 수 집계
    """분배내역 업데이트 (SKU별 텍스트)"""
    if not sku_note_map:
        return 0
    now = dt.datetime.now().isoformat(timespec="seconds")
    rows = [
        (note or "", now, snapshot_date, sku)
        for sku, note in sku_note_map.items()
    ]
    n0 = conn.total_changes
    conn.executemany(
        """
        UPDATE snapshots
        SET distribution_note = ?, updated_at = ?
        WHERE snapshot_date = ? AND sku = ?
        """,
        rows,
    )
    conn.commit()
    return conn.total_changes - n0


def update_warehouse_stock(
    conn: sqlite3.Connection,
    snapshot_date: str,
    sku_warehouse_map: dict,
    warehouse_num: int = 0,
    solid_map: Optional[dict] = None,
    assort_map: Optional[dict] = None,
    assort_ratio_map: Optional[dict] = None,
    assort_box_map: Optional[dict] = None,
) -> int:
    # [최적화] executemany 후 합계 UPDATE; 배치 반영 건수는 total_changes 차이로 집계
    """물류재고 업데이트 (warehouse_num: 0=전체, 1=항만, 2=부평)"""
    updated = 0
    now = dt.datetime.now().isoformat(timespec="seconds")
    use_detail_maps = (
        solid_map is not None
        or assort_map is not None
        or assort_ratio_map is not None
        or assort_box_map is not None
    )
    sm = solid_map or {}
    am = assort_map or {}
    arm = assort_ratio_map or {}
    abm = assort_box_map or {}

    if sku_warehouse_map:
        n0 = conn.total_changes
        if warehouse_num == 1:
            if use_detail_maps:
                rows = []
                for sku, warehouse_stock in sku_warehouse_map.items():
                    w_solid = int(sm.get(sku, 0) or 0)
                    w_assort = int(am.get(sku, 0) or 0)
                    ratio = int(arm.get(sku, 0) or 0)
                    box_ct = int(abm.get(sku, 0) or 0)
                    rows.append(
                        (
                            warehouse_stock,
                            w_solid,
                            w_assort,
                            ratio,
                            ratio,
                            box_ct,
                            box_ct,
                            now,
                            snapshot_date,
                            sku,
                        )
                    )
                conn.executemany(
                    """
                    UPDATE snapshots
                    SET warehouse1_stock = ?,
                        warehouse1_solid = ?,
                        warehouse1_assort = ?,
                        assort_ratio = CASE WHEN ? > 0 THEN ? ELSE assort_ratio END,
                        assort_box_count = CASE WHEN ? > 0 THEN ? ELSE assort_box_count END,
                        updated_at = ?
                    WHERE snapshot_date = ? AND sku = ?
                    """,
                    rows,
                )
            else:
                rows = [
                    (warehouse_stock, now, snapshot_date, sku)
                    for sku, warehouse_stock in sku_warehouse_map.items()
                ]
                conn.executemany(
                    """
                    UPDATE snapshots
                    SET warehouse1_stock = ?, updated_at = ?
                    WHERE snapshot_date = ? AND sku = ?
                    """,
                    rows,
                )
        elif warehouse_num == 2:
            if use_detail_maps:
                rows = []
                for sku, warehouse_stock in sku_warehouse_map.items():
                    w_solid = int(sm.get(sku, 0) or 0)
                    w_assort = int(am.get(sku, 0) or 0)
                    ratio = int(arm.get(sku, 0) or 0)
                    box_ct = int(abm.get(sku, 0) or 0)
                    rows.append(
                        (
                            warehouse_stock,
                            w_solid,
                            w_assort,
                            ratio,
                            ratio,
                            box_ct,
                            box_ct,
                            now,
                            snapshot_date,
                            sku,
                        )
                    )
                conn.executemany(
                    """
                    UPDATE snapshots
                    SET warehouse2_stock = ?,
                        warehouse2_solid = ?,
                        warehouse2_assort = ?,
                        assort_ratio = CASE WHEN ? > 0 THEN ? ELSE assort_ratio END,
                        assort_box_count = CASE WHEN ? > 0 THEN ? ELSE assort_box_count END,
                        updated_at = ?
                    WHERE snapshot_date = ? AND sku = ?
                    """,
                    rows,
                )
            else:
                rows = [
                    (warehouse_stock, now, snapshot_date, sku)
                    for sku, warehouse_stock in sku_warehouse_map.items()
                ]
                conn.executemany(
                    """
                    UPDATE snapshots
                    SET warehouse2_stock = ?, updated_at = ?
                    WHERE snapshot_date = ? AND sku = ?
                    """,
                    rows,
                )
        else:
            rows = [
                (warehouse_stock, now, snapshot_date, sku)
                for sku, warehouse_stock in sku_warehouse_map.items()
            ]
            conn.executemany(
                """
                UPDATE snapshots
                SET warehouse_stock = ?, updated_at = ?
                WHERE snapshot_date = ? AND sku = ?
                """,
                rows,
            )
        conn.commit()
        updated = conn.total_changes - n0

    conn.execute(
        """
        UPDATE snapshots
        SET warehouse_stock = COALESCE(warehouse1_stock, 0) + COALESCE(warehouse2_stock, 0)
        WHERE snapshot_date = ?
        """,
        (snapshot_date,),
    )
    conn.commit()

    return updated


def load_latest(conn: sqlite3.Connection) -> tuple[Optional[str], pd.DataFrame]:
    cur = conn.execute("SELECT MAX(snapshot_date) FROM snapshots")
    latest = cur.fetchone()[0]
    if not latest:
        return None, pd.DataFrame()
    df = pd.read_sql_query(SNAPSHOT_SELECT_SQL, conn, params=(latest,))
    return latest, df


def load_history(conn: sqlite3.Connection, sku: str) -> pd.DataFrame:
    return pd.read_sql_query(
        "SELECT snapshot_date, stock FROM snapshots WHERE sku = ? ORDER BY snapshot_date",
        conn,
        params=(sku,),
    )


def compute_daily_change(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return history
    h = history.copy()
    h["snapshot_date"] = pd.to_datetime(h["snapshot_date"])
    h = h.sort_values("snapshot_date")
    h["delta"] = h["stock"].diff()
    return h


def avg_daily_usage_from_history(history: pd.DataFrame) -> float:
    if history.empty or len(history) < 2:
        return 0.0
    h = compute_daily_change(history)
    deltas = h["delta"].dropna()
    usage = (-deltas[deltas < 0]).astype(float)
    if usage.empty:
        return 0.0
    return float(usage.mean())


def reorder_suggestion(
    stock: int,
    min_stock: int,
    lead_time_days: int,
    safety_stock: int,
    avg_daily_usage: float,
    target_cover_days: int,
) -> tuple[int, int]:
    rp_usage = int(round(lead_time_days * avg_daily_usage)) + safety_stock
    reorder_point = max(min_stock, rp_usage)

    target = int(round((lead_time_days + target_cover_days) * avg_daily_usage)) + safety_stock
    target = max(target, min_stock)
    suggested = max(0, target - int(stock))
    return reorder_point, suggested

