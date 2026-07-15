use crate::diagnostic::{Result, err, err_with_source};
pub(super) const TARGET_REGION_COUNT: usize = 11;
const TARGET_REGION_KEYS: [&str; TARGET_REGION_COUNT] = [
    "대전대덕구",
    "대전동구",
    "대전서구",
    "대전유성구",
    "대전중구",
    "세종시",
    "충북청주시",
    "충남공주시",
    "충남보령시",
    "충남아산시",
    "충남천안시",
];
const DAEJEON_DISTRICT_KEYS: [&str; 5] = ["대덕구", "동구", "서구", "유성구", "중구"];
pub(super) const TARGET_REGION_LABELS: [&str; TARGET_REGION_COUNT] = [
    "대전 대덕구",
    "대전 동구",
    "대전 서구",
    "대전 유성구",
    "대전 중구",
    "세종",
    "청주",
    "공주",
    "보령",
    "아산",
    "천안",
];
const ADDRESS_KEY_REPLACEMENTS: [AddressKeyReplacement; 4] = [
    AddressKeyReplacement {
        from: "충청남도",
        to: "충남",
    },
    AddressKeyReplacement {
        from: "충청북도",
        to: "충북",
    },
    AddressKeyReplacement {
        from: "대전광역시",
        to: "대전",
    },
    AddressKeyReplacement {
        from: "세종특별자치시",
        to: "세종",
    },
];
struct AddressKeyReplacement {
    from: &'static str,
    to: &'static str,
}
#[derive(Clone, Copy)]
pub(super) enum TargetRegionPolicy {
    Flexible,
    StrictSource,
}
const fn ignored_address_key_char(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, '(' | ')' | '[' | ']' | '{' | '}' | ',' | '.')
}
pub(super) fn normalize_address_key(addr: &str) -> Result<String> {
    let mut out = String::new();
    normalize_address_key_into(addr, &mut out)?;
    Ok(out)
}
fn normalize_address_key_into(addr: &str, out: &mut String) -> Result<()> {
    let mut rest = addr.trim();
    let capacity = rest.len();
    out.clear();
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source("주소 key 정규화 메모리 확보 실패", source))?;
    while !rest.is_empty() {
        let mut replaced = false;
        if matches!(rest.chars().next(), Some('충' | '대' | '세')) {
            'replacement: for rule in ADDRESS_KEY_REPLACEMENTS {
                let mut prefix_tail = rest;
                for expected in rule.from.chars() {
                    loop {
                        let Some(ch) = prefix_tail.chars().next() else {
                            continue 'replacement;
                        };
                        let Some(tail) = prefix_tail.get(ch.len_utf8()..) else {
                            continue 'replacement;
                        };
                        prefix_tail = tail;
                        if ignored_address_key_char(ch) {
                            continue;
                        }
                        if ch != expected {
                            continue 'replacement;
                        }
                        break;
                    }
                }
                out.push_str(rule.to);
                rest = prefix_tail;
                replaced = true;
                break;
            }
        }
        if replaced {
            continue;
        }
        let mut chars = rest.chars();
        let Some(ch) = chars.next() else {
            break;
        };
        rest = chars.as_str();
        if ignored_address_key_char(ch) {
            continue;
        }
        out.push(ch);
    }
    Ok(())
}
pub(super) fn target_region_index(
    region: &str,
    address: &str,
    scratch: &mut String,
    policy: TargetRegionPolicy,
) -> Result<Option<usize>> {
    if matches!(policy, TargetRegionPolicy::StrictSource) {
        normalize_address_key_into(region, scratch)?;
        let region_index = TARGET_REGION_KEYS
            .iter()
            .position(|key| scratch.as_str() == *key);
        let mut tokens = address.split_whitespace();
        let address_index = match tokens.next() {
            Some("대전") => match tokens.next() {
                Some("광역시") => tokens.next().and_then(daejeon_district_index),
                district => district.and_then(daejeon_district_index),
            },
            Some("대전광역시") => tokens.next().and_then(daejeon_district_index),
            Some("세종" | "세종시" | "세종특별자치시") => Some(5),
            Some("충북" | "충청북도") => (tokens.next() == Some("청주시")).then_some(6),
            Some("충남" | "충청남도") => match tokens.next() {
                Some("공주시") => Some(7),
                Some("보령시") => Some(8),
                Some("아산시") => Some(9),
                Some("천안시") => Some(10),
                Some("천안") if tokens.next() == Some("서북구") => Some(10),
                Some(_) | None => None,
            },
            Some(_) | None => None,
        };
        return match (region_index, address_index) {
            (Some(region_match), Some(address_match)) if region_match == address_match => {
                Ok(Some(address_match))
            }
            (Some(_), Some(_)) => Err(err(format!(
                "Opinet 소스의 지역 값과 주소가 서로 다른 대상 지역을 가리킵니다: region={region}, address={address}"
            ))),
            (Some(_), None) => Err(err(format!(
                "Opinet 소스의 지역 값은 대상 지역이지만 주소는 대상 지역이 아닙니다: region={region}, address={address}"
            ))),
            (None, Some(_)) => Err(err(format!(
                "Opinet 소스의 주소는 대상 지역이지만 지역 값이 예상 형식과 다릅니다: region={region}, address={address}"
            ))),
            (None, None) => Ok(None),
        };
    }
    normalize_address_key_into(region, scratch)?;
    if let Some(index) = target_region_index_from_normalized(scratch.as_str()) {
        return Ok(Some(index));
    }
    let region_is_daejeon = scratch.as_str() == "대전";
    normalize_address_key_into(address, scratch)?;
    if let Some(index) = target_region_index_from_normalized(scratch.as_str()) {
        return Ok(Some(index));
    }
    if region_is_daejeon {
        return Ok(DAEJEON_DISTRICT_KEYS
            .iter()
            .position(|district| scratch.starts_with(district)));
    }
    Ok(None)
}
fn daejeon_district_index(district: &str) -> Option<usize> {
    DAEJEON_DISTRICT_KEYS
        .iter()
        .position(|candidate| district == *candidate)
}
pub(super) fn increment_target_region_count(
    counts: &mut [usize; TARGET_REGION_COUNT],
    region_index: usize,
    context: &'static str,
) -> Result<()> {
    let Some(region_count) = counts.get_mut(region_index) else {
        return Err(err(format!("{context} index 범위 오류")));
    };
    *region_count = region_count
        .checked_add(1)
        .ok_or_else(|| err(format!("{context} 계산 중 overflow가 발생했습니다.")))?;
    Ok(())
}
fn target_region_index_from_normalized(text: &str) -> Option<usize> {
    if let Some(index) = TARGET_REGION_KEYS
        .iter()
        .position(|key| text.starts_with(key))
    {
        return Some(index);
    }
    if text.starts_with("세종") {
        return Some(5);
    }
    if text.starts_with("충북청주") || text.starts_with("청주") {
        return Some(6);
    }
    if text.starts_with("충남공주") || text.starts_with("공주") {
        return Some(7);
    }
    if text.starts_with("충남보령") || text.starts_with("보령") {
        return Some(8);
    }
    if text.starts_with("충남아산") || text.starts_with("아산") {
        return Some(9);
    }
    if text.starts_with("충남천안") || text.starts_with("천안") {
        return Some(10);
    }
    None
}
