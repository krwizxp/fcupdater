# fcupdater

`fuel_cost_chungcheong.xlsx`의 주유소 정보를 `지역_위치별(주유소)*.xls/.xlsx` 소스 파일로 현행화하는 CLI 도구입니다.

- Excel 미설치 환경에서 동작
- `.xls` / `.xlsx` 소스 입력 지원
- 주소 기반 매칭으로 기존 행 갱신
- 소스 기준 신규 업체 자동 추가
- 소스 미존재 업체(폐업) 자동 삭제
- `변경내역` 시트에 `변경내용` 사유 기록

## 요구사항

- Rust 1.85.0 이상

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
- `지역_위치별(주유소) (1).xls` 등 추가 소스 파일

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
- `-h, --help`: 도움말
- `--version`: 버전 표시
- `--in-place` 와 `--output` 은 동시에 사용할 수 없음

예시:

```bash
fcupdater.exe --master "C:\path\fuel_cost_chungcheong.xlsx" --sources-dir "C:\path\sources" --output out.xlsx
```

## 동작 기준

- 주소 문자열은 공백/괄호/일부 시도 표기 차이를 정규화해 매칭합니다.
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
