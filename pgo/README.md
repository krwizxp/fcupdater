# 적용된 PGO 프로파일

각 플랫폼의 실제 제품 실행 파일에서 수집한 프로파일만 적용합니다. 진단용 프로파일은 포함하지 않습니다. 프로파일을 만든 제품 소스 commit과 SHA256은 manifest.json에 기록했습니다. 제품 Rust 소스, Cargo.lock 및 release의 opt3/fat-LTO/CGU1/panic-abort/strip 설정은 유지합니다. PGO Cargo 설정은 build-pgo 명령에서만 로드되며 일반 개발·테스트 빌드는 영향을 받지 않습니다.

학습은 실제 제품 CLI의 정상/오류 경로와 공통·학습용 워크북을 사용했습니다. 검증에는 학습에서 제외한 입력과 이미 갱신된 워크북도 포함했습니다. Windows와 Linux 프로파일을 공유하거나 합치지 않습니다. 프로파일이 없는 타깃에 다른 타깃의 프로파일을 대입하지 않습니다.

## 2026-10-09 재학습과 검증

Rust 1.99.0 / LLVM 23.1.1의 일반 release 비교로 API 치환을 결정한 뒤, 변경된 실제 제품 CLI에서 Linux·Windows 프로파일을 각각 다시 수집했습니다. 진단용 프로브·고정 응답 재생 도구는 제품과 프로파일 학습 실행 파일에 포함하지 않습니다. 기존 release·타깃·보안 설정은 유지합니다.

일반 release 코드 검증은 Linux·Windows·macOS x64/ARM64 모두 통과했습니다. 현재/이전 워크북의 실제 CLI 현행화·저장 검증을 고정 Opinet 응답으로 비교하며, 실서비스 네트워크 지연과 로컬 처리 비용을 구분합니다. 새 프로파일의 성능 비교와 SHA256·원본 제품 소스 식별자는 manifest.json 및 아래 실행에 기록합니다. Windows 오류 응답 5개(헤더 index, 크기 probe, UTF-16 길이, cookie index, 전체 전송 timeout)도 기준과 동일하게 거부하고 원본 워크북을 보존했습니다.

현재/이전 워크북 각각 128개 무작위 paired 표본(표본당 실제 CLI 4회)으로 기존 PGO와 비교했습니다. 처리 시간 변화의 paired 중앙값은 Linux +0.30~0.34%, Windows +0.29~0.90%였으며, 95% 신뢰구간 상한은 모두 사전에 정한 +3% 이내였습니다. 이번 치환으로 속도가 개선되었다고 주장하지 않습니다. 실행 파일은 Linux 2,984바이트, Windows 5,120바이트 작아졌습니다. 학습에서 제외한 워크북의 저장 결과와 요약도 동일했습니다.

검증 실행:
https://github.com/krwizxp/fcupdater/actions/runs/37938918839
https://github.com/krwizxp/fcupdater/actions/runs/37940161352
https://github.com/krwizxp/fcupdater/actions/runs/37941929175

## 이전 프로파일의 측정 이력과 한계

고정 HTTP 응답을 재생한 로컬 처리 경로에서 Linux wall time은 약 17.6~22.3%, Windows는 약 11.2~14.4% 개선됐습니다. 해당 경로의 동작·메모리 허용 기준은 통과했습니다. 실서비스 다운로드 전체 지연의 개선을 측정한 결과는 아닙니다. Linux 실제 다운로드는 성공했지만 Windows에서는 기준/PGO 모두 nfl.opinet.co.kr의 WinHTTP 보안 채널 오류(12175, secure failure 0x80000000)로 실패했습니다. 이 기존 네트워크 오류의 해결이나 Windows 실제 다운로드 성공은 이번 프로파일 적용에 포함하지 않습니다. TLS/인증서 검증은 유지했습니다. Windows PGO 실행 파일 크기는 11.61% 증가했습니다.

성능 비교는 각 호스트 안에서 기준/PGO 순서를 무작위로 섞은 paired 측정입니다. CPU·파일시스템·플랫폼이 달라 동일한 개선율을 보장하지 않습니다. 공개 입력의 고정 응답 재생은 학습·측정에만 사용했습니다. 재생 DLL/SO, 진단 실행 파일과 고정 응답을 제품에 포함하지 않습니다.

소스 빌드/패키징은 Rustup과 최신 stable Rust를 사용합니다. CI와 수동 실행 워크플로는 실행 시 stable을 업데이트하며, 로컬에서는 `rustup update stable` 후 빌드합니다. manifest.json의 Rust 1.99.0 / LLVM 23.1.1은 저장된 프로파일의 학습 이력으로 유지합니다. 컴파일러가 업데이트되면 프로파일 호환성·동작·성능을 재검증하고 필요하면 재학습해야 하며, CI는 프로파일을 자동 재학습하지 않습니다. 최종 실행 파일에는 프로파일 파일이나 LLVM 도구가 필요하지 않습니다.

`build-pgo`는 호스트 타깃을 명시하여 학습 당시의 Cargo 함수 식별자를 유지합니다. 출력 경로는 `target/<host-triple>/release/`이며, CI는 그 위치의 제품 바이너리를 패키징합니다. 일반 빌드의 `target/release/` 바이너리와 혼동하지 마십시오.
