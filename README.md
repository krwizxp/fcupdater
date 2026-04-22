# fcupdater
`fuel_cost_chungcheong.xlsx`의 주유소 정보를 Opinet 전국 현재 판매가격 소스와 수동 `.xls/.xlsx` 소스 파일로 현행화하는 CLI 도구입니다.
- Excel 미설치 환경에서 동작
- Opinet 전국 단일 소스 파일 자동 다운로드(기본)
- `.xls` / `.xlsx` 소스 입력 지원
- 주소 기반 매칭으로 기존 행 갱신
- 소스 파일 prefix 대소문자 구분 없이 자동 탐색
- 소스 기준 신규 업체 자동 추가
- 소스 미존재 업체(폐업) 자동 삭제
- `변경내역` 시트에 `변경내용` 사유 기록
- 소스 주소 중복 충돌 건수/예시 요약 표시
- 출력 파일명 충돌 시 자동으로 새 파일명 선택(`_1`, `_2` suffix)
- 저장 후 OOXML 필수 파트 무결성 검증(기본)
## 요구사항
- MSRV: Rust 1.95
- 최신 Rust stable 사용 가능
- Chrome 또는 Edge 설치
- `chromedriver` 또는 `msedgedriver`가 PATH에 있거나 프로젝트 내 `chromedriver/chromedriver(.exe)` 또는 `edgedriver/msedgedriver(.exe)` 위치에 있어야 함
- Chrome 사용 시 Chrome과 ChromeDriver, Edge 사용 시 Edge와 EdgeDriver의 메이저 버전이 서로 같아야 함
- 압축/해제 도구
- Windows: `pwsh` 또는 `powershell` 또는 `tar`
- macOS/Linux: 해제는 `unzip` 또는 `python3`/`python`(zipfile), 생성은 `zip` 또는 `python3`/`python`(zipfile)
- macOS/Linux 현행화 일자: `date` 명령 우선 사용, 실패 시 `python3`/`python` 시도, 둘 다 불가하면 UTC 날짜로 대체
- macOS/Linux CP949 디코딩 정확도 향상(권장): `iconv` 또는 `python3`/`python`
## 빌드
```bash
cargo build --release
```
실행 파일:
- Windows: `target\release\fcupdater.exe`
- macOS/Linux: `target/release/fcupdater`
## GitHub Actions 실행파일
`.github/workflows/ci.yml`은 `ubuntu-latest`, `windows-latest`에서 각각 `cargo build --release --locked`를 수행하고 결과 실행 파일을 Artifact로 업로드합니다.
- Linux Artifact 파일: `fcupdater-linux-x64.tar.gz`
  - 내부 실행 파일: `fcupdater-linux-x64`
- Windows Artifact 파일: `fcupdater-windows-x64.exe`
Linux는 실행 권한 보존을 위해 바이너리를 `tar.gz`로 묶어 업로드합니다. Windows는 빌드 결과를 `fcupdater-windows-x64.exe`로 이름만 바꿔 그대로 업로드합니다.
## GitHub Actions 현행화 실행
`.github/workflows/update_master.yml`은 GitHub Actions에서 마스터 엑셀을 직접 현행화하고 결과 `.xlsx`를 Artifact로 받는 수동 실행 워크플로입니다.
- Runner: `windows-latest`
- Rust: 최신 stable을 설치해 `cargo build --release --locked` 수행
- 캐시: 없음
- 브라우저/드라이버: `skip_download: false`일 때 GitHub runner에 기본 설치된 최신 Chrome/Edge 및 대응 WebDriver를 우선 사용
- 결과물: `artifacts/<artifact_name>.xlsx` (기본: `artifacts/fcupdater-result.xlsx`)
실행 방법:
1. GitHub 저장소의 `Actions` 탭에서 `Update Master Excel` 워크플로를 선택합니다.
2. `Run workflow`를 눌러 입력값을 확인한 뒤 실행합니다.
3. 실행 완료 후 Workflow run 화면의 `Artifacts`에서 결과 파일을 다운로드합니다.
입력값:
- `master_path`: 저장소 안에 있는 기준 엑셀 경로 (기본: `fuel_cost_chungcheong.xlsx`)
- `sources_prefix`: 소스 파일명 prefix (경로 아님, 기본: `현재 판매가격(주유소)`)
- `skip_download`: `true`이면 Opinet 자동 다운로드를 생략하고 저장소 안의 기존 소스 파일만 사용
- `no_change_log`: `true`이면 `변경내역` 시트를 갱신하지 않음
- `fast_save`: `true`이면 저장 후 무결성 재검증 생략
- `artifact_name`: 업로드할 결과 파일 이름 prefix (`<artifact_name>.xlsx`로 업로드)
주의:
- 기본값(`skip_download: false`)은 runner에서 Opinet 자동 다운로드 후 바로 현행화를 수행하므로 별도 소스 파일 업로드가 필요 없습니다.
- `skip_download: true`를 사용하려면 저장소 안에 `현재 판매가격(주유소)*.xls/.xlsx` 파일이 있어야 합니다.
- GitHub Actions 수동 실행 화면은 임의 파일 업로드를 직접 받지 않으므로, 기준 엑셀과 수동 소스 파일을 쓰려면 먼저 저장소에 커밋해 두어야 합니다.
- 결과 파일은 저장소에 커밋되지 않고 Artifact로만 업로드됩니다.
## 빠른 사용
1. 아래 파일과 실행 환경을 준비합니다.
- `fuel_cost_chungcheong.xlsx`
- Chrome 또는 Edge
- `chromedriver` 또는 `msedgedriver` (`PATH` 등록)
2. 실행합니다.
```bash
fcupdater.exe
```
3. 실행 중 Opinet에서 전국 현재 판매가격 파일 1건을 자동 다운로드하고, 11개 대상 지역만 필터링해 현행화를 진행합니다. 기본 소스 prefix는 `현재 판매가격(주유소)`입니다. 브라우저는 Chrome 우선, 실패 시 Edge로 자동 폴백합니다.
4. 기본 출력 파일:
- `fuel_cost_chungcheong_updated_YYYY-MM-DD.xlsx`
수동 소스 파일만 사용하려면:
```bash
fcupdater.exe --skip-download
```
## 옵션
- `--master <PATH>`: 마스터 파일 경로
- `--sources-dir <PATH>`: 소스 폴더 경로, 자동 다운로드 저장 폴더
- `--sources-prefix <TEXT>`: 소스 파일명 prefix (경로 아님, 기본: `현재 판매가격(주유소)`)
- `--skip-download`: Opinet 자동 다운로드 생략, 기존 소스 파일만 사용
- `--output <PATH>`: 출력 파일 경로
- `--in-place`: 마스터 파일 덮어쓰기(백업 자동 생성)
- `--no-change-log`: `변경내역` 시트 갱신 안 함
- `--dry-run`: 파일 저장 없이 요약만 출력
- `--fast-save`: 저장 후 무결성 재검증 생략(속도 우선)
- `-h, --help`: 도움말
- `--version`: 버전 표시
- `--in-place` 와 `--output` 은 동시에 사용할 수 없음
- `--dry-run` 과 `--fast-save` 는 동시에 사용할 수 없음
- `--sources-prefix` 는 파일명 접두어만 허용하며, 경로 구분자(`/`, `\`), Windows 금지 문자(`< > : " | ? *`), 끝 공백/점은 사용할 수 없음
환경 변수:
- `FCUPDATER_SOURCE_HEADER_SCAN_ROWS`: 소스 헤더 탐색 최대 행 수(기본 200, 최대 10000)
- `FCUPDATER_MASTER_HEADER_SCAN_ROWS`: 유류비 마스터 헤더 탐색 최대 행 수(기본 200, 최대 20000)
- `FCUPDATER_CHANGELOG_HEADER_SCAN_ROWS`: 변경내역 헤더 탐색 최대 행 수(기본 30, 최대 1000)
- `FCUPDATER_CHANGELOG_HEADER_SCAN_COLS`: 변경내역 헤더 탐색 최대 열 수(기본 60, 최대 500)
- `FCUPDATER_CHANGELOG_STYLE_TEMPLATE_ROW`: 변경내역 서식 복제 기준 행(기본 243)
- `FCUPDATER_CP949_STRICT`: `1/true/yes/on`일 때 CP949 디코딩 실패 시 대체문자 처리 대신 즉시 오류 반환
- `FCUPDATER_DURABILITY_STRICT`: `1/true/yes/on`이면 비Windows 저장 후 `sync_all` 실패를 경고가 아닌 오류로 처리
- `FCUPDATER_COMMAND_TIMEOUT_SECS`: 압축/해제 등 외부 명령 제한 시간(초). 미설정/0이면 제한 없음(기본)
- `FCUPDATER_DECODER_TIMEOUT_SECS`: CP949 디코더 외부 명령 제한 시간(초). 미설정/0이면 제한 없음(기본)
예시:
```bash
fcupdater.exe --master "C:\path\fuel_cost_chungcheong.xlsx" --sources-dir "C:\path\sources" --output out.xlsx
fcupdater.exe --skip-download --sources-dir "C:\path\manual-sources" --output out.xlsx
```
## 동작 기준
- 기본 실행은 Opinet에서 전국 현재 판매가격 파일 1건을 자동 다운로드한 뒤, 11개 대상 지역만 필터링해 현행화를 진행합니다.
- `--sources-dir`는 자동 다운로드 결과 저장 위치이기도 합니다.
- 자동 다운로드 파일명은 `{prefix}__fcupdater_auto__...` 형식이며, 해당 파일이 있으면 그 파일들만 우선 사용합니다.
- `--skip-download`를 지정하면 기존 `현재 판매가격(주유소)*.xls/.xlsx` 파일만 사용합니다.
- 주소 문자열은 공백/괄호/일부 시도 표기 차이를 정규화해 매칭합니다.
- `--sources-prefix`는 파일명 접두사 매칭 시 대소문자를 구분하지 않습니다.
- `--sources-prefix`는 경로가 아닌 파일명 접두어만 허용합니다.
- 매칭된 기존 업체는 다음 정보를 소스 기준으로 갱신합니다.
- 상호, 상표, 셀프여부, 주소, 휘발유/고급유/경유 가격
- 소스에는 있고 기준 파일에는 없으면 신규로 추가합니다.
- 소스에는 없고 기준 파일에는 있으면 폐업으로 간주하여 삭제합니다.
## 변경내역 시트 기록
`변경내역` 시트의 헤더를 읽어 컬럼을 동적으로 찾아 기록합니다.
필수 헤더:
- `지역`, `상호`, `주소`, `변경내용`
- `휘발유(이전)`, `휘발유(신규)`
- `고급유(이전)`, `고급유(신규)`
- `경유(이전)`, `경유(신규)`
선택 헤더:
- `휘발유 Δ`, `고급유 Δ`, `경유 Δ` (또는 유사 표기)
`변경내용` 사유 예시:
- `가격변동`
- `상호변경`
- `상표변경`
- `셀프여부변경`
- `주소변경`
- `신규`
- `폐업`
복수 사유가 동시에 발생하면 쉼표로 함께 기록됩니다.
