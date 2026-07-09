use crate::diagnostic::{Result, err, err_with_source};
use alloc::string::String;
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
) -> Result<Option<usize>> {
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
        return Ok(target_daejeon_district_index(scratch.as_str()));
    }
    Ok(None)
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
    if let Some(district) = text.strip_prefix("대전광역시") {
        return target_daejeon_district_index(district);
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
    target_daejeon_district_index(text)
}
fn target_daejeon_district_index(text: &str) -> Option<usize> {
    if text.starts_with("대덕구") {
        Some(0)
    } else if text.starts_with("동구") {
        Some(1)
    } else if text.starts_with("서구") {
        Some(2)
    } else if text.starts_with("유성구") {
        Some(3)
    } else if text.starts_with("중구") {
        Some(4)
    } else {
        None
    }
}
