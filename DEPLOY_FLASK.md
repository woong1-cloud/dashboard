# Flask 대시보드(dashboard_v2.py) 배포 가이드 — UI 수정 없이

**배포 표기 버전: V4** (`APP_TITLE`). Gunicorn 진입점은 **`dashboard_v2:app`**(파일명 `dashboard_v2.py`) 그대로 두면 됩니다.

이 문서는 **Flask 버전(dashboard_v2.py)** 을 **UI 변경 없이** 그대로 배포하는 단계별 가이드입니다.  
**Railway** 기준으로 작성했으며, 무료/유료 여부도 정리했습니다.

---

## ⚠️ Railway 요금 요약 (필수 확인)

| 항목 | 내용 |
|------|------|
| **무료 체험** | 가입 후 **$5 크레딧**, **30일** 동안 사용 가능 (신용카드 없이 가능). |
| **체험 종료 후** | 크레딧 소진 또는 30일 경과 후 **유료 전환 필요**. 상시 무료 플랜은 없음. |
| **유료 플랜** | **Hobby $5/월** (약 $5 상당 리소스 포함), 사용량 초과 시 추가 과금. |
| **결론** | **상시 무료 배포는 불가.** 30일 체험 후에는 **월 약 $5 이상** 예상. |

소규모 Flask 앱은 Hobby $5 안에서 사용 가능한 경우가 많지만, **“완전 무료”로 계속 쓰려면** Render 무료 플랜, Fly.io, 또는 Streamlit Cloud(Streamlit 버전) 등을 고려하세요.

---

## 사전 준비 (로컬/저장소)

- [x] **Procfile**  
  - 이미 있음: `web: gunicorn dashboard_v2:app --bind 0.0.0.0:$PORT`
- [x] **requirements.txt**  
  - `gunicorn`, `flask`, `pandas`, `openpyxl`, `plotly` 등 포함
- [x] **Flask 앱 진입점**  
  - `dashboard_v2:app` (gunicorn이 이걸 사용)
- [ ] **GitHub 저장소**  
  - 배포할 코드가 올라가 있어야 함 (예: `woong1-cloud/dashboard` 또는 전용 저장소)

---

## 1단계: 배포용 저장소 확인

Flask 배포에 필요한 파일이 모두 포함되어 있는지 확인하세요.

- `dashboard_v2.py`
- `inventory_core.py`
- `Procfile`
- `requirements.txt`
- `templates/` (전체)
- `static/` (있다면)

**실행 (선택):**  
저장소 루트에서 다음이 있는지 확인합니다.

```bash
# PowerShell
dir dashboard_v2.py, inventory_core.py, Procfile, requirements.txt
dir templates
```

---

## 2단계: Railway 계정 및 프로젝트

1. [railway.app](https://railway.app) 접속 → **Login** → GitHub로 로그인.
2. **New Project** 선택.
3. **Deploy from GitHub repo** 선택 후, 사용할 저장소 연결  
   (예: `woong1-cloud/dashboard`).
4. 연결 후 **브랜치** 선택 (예: `main` 또는 `master`).

---

## 3단계: 서비스 설정 (Flask로 인식시키기)

1. 생성된 **Service** 클릭.
2. **Settings** 탭으로 이동.
3. **Build**  
   - **Builder**: Nixpacks (기본값 유지).  
   - **Build Command**: 비워두거나, 필요 시 `pip install -r requirements.txt` (보통 자동 감지).
   - **Root Directory**: 비워두면 저장소 루트 사용.  
     (앱이 서브폴더에 있으면 해당 폴더 지정.)
4. **Deploy**  
   - **Start Command**: 비워두면 **Procfile**의 `web:` 명령이 사용됨.  
     → `gunicorn dashboard_v2:app --bind 0.0.0.0:$PORT` 가 실행되어 Flask가 올라갑니다.  
   - **Restart Policy**: 필요 시 설정 (기본값 유지해도 됨).

Procfile이 있으면 **Start Command를 비워두는 것**이 중요합니다. 비어 있어야 Procfile이 적용됩니다.

---

## 4단계: 환경 변수 설정

**Variables** 탭에서 다음을 추가합니다.

| 변수명 | 값 | 비고 |
|--------|-----|------|
| `DEPLOY_MODE` | `1` | 배포 모드(초기화 비노출 등) |
| `FLASK_SECRET_KEY` | 랜덤 문자열 | 세션/쿠키 암호화용, **반드시 설정** |
| `PORT` | (설정 안 함) | Railway가 자동으로 넣어 줌 |

**FLASK_SECRET_KEY** 예시 생성 (로컬에서 한 번만 실행):

```powershell
# PowerShell
-join ((48..57) + (65..90) + (97..122) | Get-Random -Count 32 | % {[char]$_})
```

---

## 5단계: 배포 및 도메인

1. **Deploy** 버튼으로 배포 시작 (또는 GitHub push 시 자동 배포).
2. **Settings → Networking → Generate Domain** 으로 공개 URL 생성  
   (예: `https://xxx.up.railway.app`).
3. 브라우저에서 해당 URL 접속 → 로그인 화면(기본 비밀번호 `1234`)이 나오면 **UI 수정 없이 Flask 버전이 배포된 것**입니다.

---

## 6단계: 동작 확인 체크리스트

- [ ] 로그인 페이지가 **기존 Flask 템플릿 UI** 그대로 보인다.
- [ ] 로그인 후 대시보드·업로드·백업 등 메뉴가 **기존과 동일**하다.
- [ ] 엑셀 업로드 후 대시보드에서 데이터가 보인다.
- [ ] (선택) **비밀번호 변경** 후 재로그인으로 확인.

---

## 문제 발생 시

- **Application failed to respond**  
  - Procfile의 `web:` 명령이 사용되는지 확인 (Start Command 비움).  
  - `PORT`는 Railway가 주입하므로 별도 설정하지 않아도 됨.
- **500 에러**  
  - Railway **Deployments → View Logs**에서 Python traceback 확인.  
  - `DEPLOY_MODE=1` 이면 화면에는 상세 오류가 안 보이므로, 로그로만 확인 가능.
- **DB/파일 휘발**  
  - Railway는 **에피소드(컨테이너) 재시작/재배포 시 로컬 디스크가 비워질 수 있음.**  
  - `inventory.db`는 컨테이너 내부에만 있으므로, 중요 데이터는 주기적으로 **백업/내보내기**로 다운로드해 두는 것이 좋습니다.  
  - 영구 저장이 필요하면 Railway **Volume** 연결 또는 외부 DB(S3 등) 연동을 별도로 검토해야 합니다.

---

## 요약: “UI 수정 없이 Flask 배포”가 되었는지

- 배포되는 **진입 파일**이 `dashboard_v2.py` 이고,
- **gunicorn**이 `dashboard_v2:app` 으로 실행하며,
- **Procfile**만 사용하고 **Start Command는 비어 있으면**  
→ **기존 Flask + 템플릿 UI가 그대로** 나갑니다.  
Streamlit(app.py)은 사용하지 않으므로, Streamlit용 UI로 바뀌지 않습니다.

---

## Railway 외 대안 (참고)

- **Render**  
  - 무료 플랜 있음(슬립 등 제약). Flask도 배포 가능.  
  - 동일하게 Procfile + `gunicorn dashboard_v2:app` 사용.
- **Fly.io**  
  - 소규모 앱 무료 리소스 있음. `fly.toml` 등 설정 필요.
- **Streamlit Cloud**  
  - **Streamlit용(app.py)** 전용. Flask UI와는 다르므로 “UI 수정 없이 Flask”에는 해당 없음.

이 가이드대로 진행하면 **dashboard_v2.py(Flask) 버전을 UI 수정 없이 Railway에 배포**할 수 있고, **Railway는 30일 체험 후 유료**라는 점만 꼭 기억하시면 됩니다.
