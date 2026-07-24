# fcupdater

`fuel_cost_chungcheong.xlsx`를 Opinet 전국 현재 판매가격 자료에 맞춰 현행화하는 명령행 도구입니다. Windows, Linux, macOS에서 같은 방식으로 사용할 수 있습니다.

## 주요 기능

- Opinet 주유소 판매가격 자료 다운로드
- 주소 기준 주유소 정보와 유종별 가격 갱신
- 신규·폐업 주유소 반영
- 지역, 상호, 상표, 셀프 여부 변경 반영
- 지역화폐와 스마트주유 할인 적용
- `변경내역` 시트 갱신
- 저장 결과 검증

## 지원 환경

- Rust 1.97.1 이상
- Windows 10 22H2 이상 또는 Windows 11
- Linux 및 macOS
- Linux/macOS의 libcurl 7.85.0 이상

Linux에서 직접 빌드할 때는 배포판의 libcurl 개발 패키지를 사용합니다. 현행화 날짜는 KST 기준으로 기록됩니다.

## 빌드

```bash
cargo build --release --locked
```

빌드 결과는 다음 위치에 생성됩니다.

- Windows: `target\release\fcupdater.exe`
- Linux/macOS: `target/release/fcupdater`

## 사용 방법

실행 파일과 저장소에서 제공하는 `fuel_cost_chungcheong.xlsx`를 같은 폴더에 둔 뒤 실행합니다.

```bash
fcupdater
```

프로그램은 Opinet 자료를 내려받아 대상 지역의 주유소 정보를 갱신하고 같은 워크북에 저장합니다. 저장을 시작하기 전에 워크북 구성과 주요 데이터 형식을 확인하며, 원본 상태를 확인한 뒤 안전하게 교체합니다.

### 옵션

- `-h`, `--help`: 도움말 표시
- `--verify`: 저장 결과를 다시 열어 확인한 뒤 워크북에 반영
- `--version`: 버전 표시

## 워크북

저장소에서 제공하는 워크북은 다음 두 시트로 구성됩니다.

- `유류비`: 현재 주유소 정보, 가격, 할인과 순위
- `변경내역`: 가격과 주유소 정보의 변경 이력

현행화 과정에서는 수식과 계산값, 서식, 변경 이력의 일관성을 함께 관리합니다. `--verify` 옵션은 생성된 워크북을 다시 열어 구조와 주요 내용을 한 번 더 확인합니다.
Microsoft Excel 또는 LibreOffice Calc로 저장한 제공 워크북을 사용할 수 있으며, 현행화 결과는 Microsoft Excel 형식으로 일관되게 저장됩니다.

## 대상 지역

- 대전광역시
- 세종특별자치시
- 충청북도 청주시
- 충청남도 공주시
- 충청남도 보령시
- 충청남도 아산시
- 충청남도 천안시

대전광역시는 하나의 지역으로 통합하여 지역 검증과 할인율을 적용합니다.

## 변경내역

`변경내역` 시트에는 다음 항목이 기록됩니다.

- 가격변동
- 지역정정
- 상호변경
- 상표변경
- 셀프여부변경
- 신규
- 폐업

휘발유, 고급휘발유, 경유의 이전 가격과 신규 가격, 변동액을 함께 확인할 수 있습니다.

## GitHub Actions

CI 워크플로는 Windows, Linux, Intel Mac, Apple Silicon Mac용 release build를 확인합니다. `main` 브랜치와 태그 실행에서는 배포용 Artifact를 제공하며, Pull Request에서는 같은 환경의 빌드를 검증합니다.

워크북 현행화 워크플로는 최신 Opinet 자료를 반영한 `fcupdater-result.xlsx`를 Artifact로 제공합니다.
