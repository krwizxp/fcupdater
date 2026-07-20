# fcupdater
`fuel_cost_chungcheong.xlsx`를 Opinet 전국 현재 판매가격(주유소) `.xls` 소스로 현행화하는 CLI 도구입니다.
- Excel 독립 실행 지원
- Opinet 전국 단일 `.xls` 소스 자동 다운로드
- 마스터 파일은 `fuel_cost_chungcheong.xlsx` 하나로 직접 현행화
- `.xlsx` 읽기/저장은 내장 Rust ZIP/deflate 처리 사용
- 주소 기반 매칭으로 기존 행 갱신
- 소스 기준 신규 업체 자동 추가
- 소스 기준 업체 영업 상태 정리
- `변경내역` 시트 항상 갱신
- 기본 저장은 빠른 승격 사용, `--verify` 실행 시 저장 후 OOXML 필수 파트 무결성 검증
## 요구사항
- MSRV: Rust 1.97.1
- Windows 실행 환경: Windows 10 22H2 이상, 최신 Windows 11 권장
- 자동 다운로드: Windows는 WinHTTP, Linux/macOS는 native libcurl 사용
- Linux/macOS native libcurl은 HTTPS protocol allowlist 설정을 위해 7.85.0 이상이 필요합니다.
- Linux 빌드 환경: `libcurl4-openssl-dev`
- Linux 실행 환경: `libcurl4`
- 현행화 일자: OS 시간대와 무관하게 KST 기준 날짜 사용
## 빌드
```bash
cargo build --release --locked
```
실행 파일:
- Windows: `target\release\fcupdater.exe`
- macOS/Linux: `target/release/fcupdater`
## 빠른 사용
1. 실행 파일과 같은 폴더에 `fuel_cost_chungcheong.xlsx`를 둡니다.
2. 실행합니다.
```bash
fcupdater
```
3. 프로그램이 Opinet에서 현재 판매가격 `.xls`를 다운로드하고 7개 대상 지역을 반영합니다.
4. `fuel_cost_chungcheong.xlsx`가 직접 현행화됩니다.
## CLI
지원 옵션은 아래와 같습니다.
- `-h`, `--help`: 도움말
- `--verify`: 저장 후 임시 XLSX를 재열어 검증한 뒤 대상 파일로 승격
- `--version`: 버전 표시
## GitHub Actions
`.github/workflows/ci.yml`은 `ubuntu-latest`, `macos-26-intel`, `macos-26`, `windows-latest`에서 `cargo build --release --locked`를 수행합니다. 실행 파일은 Pull Request가 아닌 `main` 브랜치 또는 태그의 신뢰된 실행에서만 다음 고유 Artifact로 업로드합니다.
- Linux Artifact `fcupdater-linux-x64`: `fcupdater-linux-x64.tar` (내부 실행 파일: `fcupdater-linux-x64`)
- macOS x64 Artifact `fcupdater-macos-x64`: `fcupdater-macos-x64.tar` (내부 실행 파일: `fcupdater-macos-x64`)
- macOS arm64 Artifact `fcupdater-macos-arm64`: `fcupdater-macos-arm64.tar` (내부 실행 파일: `fcupdater-macos-arm64`)
- Windows Artifact `fcupdater-windows-x64`: `fcupdater-windows-x64.exe`

Linux/macOS는 실행 권한 보존을 위해 바이너리를 Rust 아티팩트 도구로 `tar`에 묶어 업로드합니다. Windows는 같은 도구로 빌드 결과를 `fcupdater-windows-x64.exe`로 이름만 바꿔 그대로 업로드합니다. 실행 파일은 `CI` 워크플로의 `main` 또는 태그 push 실행에서 생성되었는지 확인한 뒤 사용하세요. Pull Request 실행이나 출처를 확인할 수 없는 산출물은 실행하지 마세요.
`.github/workflows/update_master.yml`은 `ubuntu-latest`에서 고정 워크플로를 실행합니다.
- 마스터: `fuel_cost_chungcheong.xlsx`
- Artifact 이름: `fcupdater-result`
- 포함 파일: `fcupdater-result.xlsx`
## 동작 기준
- Opinet 자동 다운로드는 항상 수행합니다.
- 자동 다운로드는 지정한 HTTPS 호스트의 직접 응답만 허용하며 HTTP 리다이렉트를 따라가지 않습니다.
- 소스 형식은 OLE2/BIFF `.xls` 기준입니다.
- BIFF `CODEPAGE = 1200` 기준으로 읽습니다.
- 소스 worksheet는 1개 구조를 기준으로 읽습니다.
- 소스 cell record는 `LABELSST` 기반 문자열 셀 구조를 기준으로 읽습니다.
- 소스 데이터는 고정 열 구조를 사용합니다.
- 주소·상호·셀프 구분과 가격 형식/범위/완전성 검증에 실패하면 저장을 중단합니다.
- 마스터 파일을 직접 저장하며 기본 실행은 저장 검증을 생략합니다.
- `--verify` 실행은 임시 XLSX 생성 후 재열기 검증을 통과한 경우에만 대상 파일로 승격합니다.
- 변경내역 시트는 항상 갱신합니다.
- 주소는 기존 업체 매칭 기준입니다. 소스 주소와 매칭되지 않는 기존 행은 폐업, 소스에만 있는 행은 신규로 처리합니다.
## 대상 지역
- 대전광역시
- 세종특별자치시
- 충청북도 청주시
- 충청남도 공주시
- 충청남도 보령시
- 충청남도 아산시
- 충청남도 천안시
## 변경내역 시트 기록
`변경내역` 시트는 3행 A~M 고정 헤더를 사용합니다.
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
변경 사유:
- `가격변동`
- `지역정정`
- `상호변경`
- `상표변경`
- `셀프여부변경`
- `신규`
- `폐업`
