# fcupdater

`fuel_cost_chungcheong.xlsx`를 Opinet 전국 현재 판매가격(주유소) 자료에 맞춰 현행화하는 CLI 도구입니다. Excel 설치 없이 Windows, Linux, macOS에서 실행할 수 있습니다.

## 주요 기능

- Opinet 전국 판매가격 `.xls` 자동 다운로드
- 주소 기준 기존 주유소 정보와 가격 갱신
- 신규·폐업 주유소 반영
- `변경내역` 시트 갱신
- XLSX 파일 직접 읽기와 저장
- 기본 빠른 저장 및 `--verify` 저장 결과 확인

## 실행 환경

- Rust 1.97.1 이상
- Windows 10 22H2 이상 또는 Windows 11
- Linux 빌드: `libcurl4-openssl-dev`
- Linux 실행: `libcurl4`
- Linux/macOS libcurl 7.85.0 이상

Windows는 WinHTTP, Linux와 macOS는 native libcurl을 사용합니다. 현행화 날짜는 실행 환경의 시간대와 관계없이 KST 기준으로 기록합니다.

## 빌드와 실행

```bash
cargo build --release --locked
```

빌드 결과는 다음 위치에 생성됩니다.

- Windows: `target\release\fcupdater.exe`
- Linux/macOS: `target/release/fcupdater`

실행 파일과 `fuel_cost_chungcheong.xlsx`를 같은 폴더에 두고 실행합니다.

```bash
fcupdater
```

프로그램이 Opinet 자료를 내려받아 7개 대상 지역을 반영하고 같은 마스터 파일을 현행화합니다.

## 옵션

- `-h`, `--help`: 도움말 표시
- `--verify`: 임시 XLSX를 저장하고 재열기 검증을 마친 뒤 마스터 파일에 반영
- `--version`: 버전 표시

## 마스터와 소스 형식

마스터는 저장소에서 제공하는 `fuel_cost_chungcheong.xlsx` 템플릿을 사용합니다. 템플릿 구성은 다음과 같습니다.

- 시트: `유류비`, `변경내역` 순서의 2개
- `유류비`: 14행 A~W 헤더
- `변경내역`: 3행 A~M 헤더
- OOXML: 기본 SpreadsheetML namespace와 표준 `r:id`
- 셀 표현: shared string cell과 일반 수식

프로그램은 현행화를 시작하기 전에 템플릿 구조와 주소·상호·셀프 구분, 가격 형식·범위·완전성을 확인합니다. Opinet 소스는 다음 형식으로 읽습니다.

- OLE2/BIFF `.xls`
- `CODEPAGE = 1200`
- worksheet 1개
- `LABELSST` 문자열 셀
- Opinet 판매가격 열 구조

다운로드 자료는 지정된 HTTPS 호스트의 직접 응답에서 가져옵니다. 기본 실행은 빠른 저장을 사용하고, `--verify`는 저장 결과를 다시 열어 OOXML 구조를 확인합니다.

## 대상 지역

- 대전광역시
- 세종특별자치시
- 충청북도 청주시
- 충청남도 공주시
- 충청남도 보령시
- 충청남도 아산시
- 충청남도 천안시

## 변경내역

`변경내역` 시트의 3행 A~M 헤더는 다음과 같습니다.

- A: `지역`
- B: `상호`
- C: `주소`
- D: `변경내용`
- E: `휘발유(이전)`
- F: `휘발유(신규)`
- G: `휘발유 Δ`
- H: `고급유(이전)`
- I: `고급유(신규)`
- J: `고급유 Δ`
- K: `경유(이전)`
- L: `경유(신규)`
- M: `경유 Δ`

변경내용은 `가격변동`, `지역정정`, `상호변경`, `상표변경`, `셀프여부변경`, `신규`, `폐업`으로 기록합니다.

## GitHub Actions

`.github/workflows/ci.yml`은 다음 4개 환경에서 release build를 확인합니다.

- `ubuntu-latest`: `fcupdater-linux-x64.tar`
- `macos-26-intel`: `fcupdater-macos-x64.tar`
- `macos-26`: `fcupdater-macos-arm64.tar`
- `windows-latest`: `fcupdater-windows-x64.exe`

Linux/macOS 실행 파일은 실행 권한을 보존하는 tar로, Windows 실행 파일은 exe로 준비합니다. `main` 브랜치와 태그 push 실행은 배포용 Artifact를 생성하고, Pull Request 실행은 같은 release build를 확인합니다.

`.github/workflows/update_master.yml`은 `ubuntu-latest`에서 마스터를 현행화하고 `fcupdater-result.xlsx` Artifact를 생성합니다.
