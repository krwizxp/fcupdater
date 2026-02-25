# fcupdater

`fuel_cost_chungcheong.xlsx`의 주유소 정보를 `지역_위치별(주유소)*.xls/.xlsx` 소스 파일로 현행화하는 CLI 도구입니다.

- Excel 미설치 환경에서 동작
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

- Rust 1.93.0 이상
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

## 빠른 사용

1. 아래 파일들을 같은 폴더에 둡니다.

- `fuel_cost_chungcheong.xlsx`
- `지역_위치별(주유소).xls`
- `지역_위치별(주유소) (1)~(...).xls` 등 추가 소스 파일

2. 실행합니다.

```bash
fcupdater.exe
```

3. 기본 출력 파일:

- `fuel_cost_chungcheong_updated_YYYY-MM-DD.xlsx`

## 옵션

- `--master <PATH>`: 마스터 파일 경로
- `--sources-dir <PATH>`: 소스 폴더 경로
- `--sources-prefix <TEXT>`: 소스 파일 prefix
- `--output <PATH>`: 출력 파일 경로
- `--in-place`: 마스터 파일 덮어쓰기(백업 자동 생성)
- `--no-change-log`: `변경내역` 시트 갱신 안 함
- `--dry-run`: 파일 저장 없이 요약만 출력
- `--fast-save`: 저장 후 무결성 재검증 생략(속도 우선)
- `-h, --help`: 도움말
- `--version`: 버전 표시
- `--in-place` 와 `--output` 은 동시에 사용할 수 없음
- `--dry-run` 과 `--fast-save` 는 동시에 사용할 수 없음

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
```

## 동작 기준

- 주소 문자열은 공백/괄호/일부 시도 표기 차이를 정규화해 매칭합니다.
- `--sources-prefix`는 파일명 접두사 매칭 시 대소문자를 구분하지 않습니다.
- 매칭된 기존 업체는 다음 정보를 소스 기준으로 갱신합니다.
- 상호, 상표, 셀프여부, 주소, 전화번호, 휘발유/고급유/경유 가격
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
- `전화번호변경`
- `신규`
- `폐업`

복수 사유가 동시에 발생하면 쉼표로 함께 기록됩니다.
