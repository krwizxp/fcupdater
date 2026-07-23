use crate::diagnostic::{Result, err, err_with_source};
pub(super) const TARGET_REGION_COUNT: usize = 7;
const DAEJEON_DISTRICT_KEYS: [&str; 5] = ["대덕구", "동구", "서구", "유성구", "중구"];
pub(super) const TARGET_REGIONS: [TargetRegion; TARGET_REGION_COUNT] = [
    TargetRegion::Daejeon,
    TargetRegion::Sejong,
    TargetRegion::Cheongju,
    TargetRegion::Gongju,
    TargetRegion::Boryeong,
    TargetRegion::Asan,
    TargetRegion::Cheonan,
];
const ADDRESS_KEY_REPLACEMENTS: [(&str, &str); 4] = [
    ("충청남도", "충남"),
    ("충청북도", "충북"),
    ("대전광역시", "대전"),
    ("세종특별자치시", "세종"),
];
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum TargetRegion {
    Asan,
    Boryeong,
    Cheonan,
    Cheongju,
    Daejeon,
    Gongju,
    Sejong,
}
#[derive(Clone, Copy)]
pub(super) enum TargetRegionPolicy {
    Flexible,
    StrictSource,
}
impl TargetRegion {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::Daejeon => "대전",
            Self::Sejong => "세종",
            Self::Cheongju => "청주",
            Self::Gongju => "공주",
            Self::Boryeong => "보령",
            Self::Asan => "아산",
            Self::Cheonan => "천안",
        }
    }
}
const fn ignored_address_key_char(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, '(' | ')' | '[' | ']' | '{' | '}' | ',' | '.')
}
pub(super) fn normalize_address_key_into(addr: &str, out: &mut String) -> Result<()> {
    let mut rest = addr.trim();
    let capacity = rest.len();
    out.clear();
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source("주소 key 정규화 메모리 확보 실패", source))?;
    while !rest.is_empty() {
        let mut replaced = false;
        if matches!(rest.chars().next(), Some('충' | '대' | '세')) {
            'replacement: for (from, to) in ADDRESS_KEY_REPLACEMENTS {
                let mut prefix_tail = rest;
                for expected in from.chars() {
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
                out.push_str(to);
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
pub(super) fn target_region(
    region: &str,
    address: &str,
    scratch: &mut String,
    policy: TargetRegionPolicy,
) -> Result<Option<TargetRegion>> {
    if matches!(policy, TargetRegionPolicy::StrictSource) {
        normalize_address_key_into(region, scratch)?;
        let normalized_region = scratch.as_str();
        let parsed_region = if normalized_region == "대전"
            || normalized_region
                .strip_prefix("대전")
                .is_some_and(|district| DAEJEON_DISTRICT_KEYS.contains(&district))
        {
            Some(TargetRegion::Daejeon)
        } else {
            match normalized_region {
                "세종시" => Some(TargetRegion::Sejong),
                "충북청주시" => Some(TargetRegion::Cheongju),
                "충남공주시" => Some(TargetRegion::Gongju),
                "충남보령시" => Some(TargetRegion::Boryeong),
                "충남아산시" => Some(TargetRegion::Asan),
                "충남천안시" => Some(TargetRegion::Cheonan),
                _ => None,
            }
        };
        let mut tokens = address.split_whitespace();
        let address_region = match tokens.next() {
            Some("대전") => match tokens.next() {
                Some("광역시") => tokens.next().and_then(daejeon_region),
                district => district.and_then(daejeon_region),
            },
            Some("대전광역시") => tokens.next().and_then(daejeon_region),
            Some("세종" | "세종시" | "세종특별자치시") => Some(TargetRegion::Sejong),
            Some("충북" | "충청북도") => {
                (tokens.next() == Some("청주시")).then_some(TargetRegion::Cheongju)
            }
            Some("충남" | "충청남도") => match tokens.next() {
                Some("공주시") => Some(TargetRegion::Gongju),
                Some("보령시") => Some(TargetRegion::Boryeong),
                Some("아산시") => Some(TargetRegion::Asan),
                Some("천안시") => Some(TargetRegion::Cheonan),
                Some("천안") if tokens.next() == Some("서북구") => Some(TargetRegion::Cheonan),
                Some(_) | None => None,
            },
            Some(_) | None => None,
        };
        return match (parsed_region, address_region) {
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
    let region_is_daejeon = scratch.as_str() == "대전";
    if !region_is_daejeon && let Some(target) = target_region_from_normalized(scratch.as_str()) {
        return Ok(Some(target));
    }
    normalize_address_key_into(address, scratch)?;
    if let Some(target) = target_region_from_normalized(scratch.as_str()) {
        return Ok(Some(target));
    }
    if region_is_daejeon {
        return Ok(DAEJEON_DISTRICT_KEYS
            .iter()
            .any(|district| scratch.starts_with(district))
            .then_some(TargetRegion::Daejeon));
    }
    Ok(None)
}
fn daejeon_region(district: &str) -> Option<TargetRegion> {
    DAEJEON_DISTRICT_KEYS
        .contains(&district)
        .then_some(TargetRegion::Daejeon)
}
pub(super) const fn increment_target_region_count(
    counts: &mut [usize; TARGET_REGION_COUNT],
    region: TargetRegion,
) {
    let &mut [
        ref mut daejeon,
        ref mut sejong,
        ref mut cheongju,
        ref mut gongju,
        ref mut boryeong,
        ref mut asan,
        ref mut cheonan,
    ] = counts;
    let region_count = match region {
        TargetRegion::Daejeon => daejeon,
        TargetRegion::Sejong => sejong,
        TargetRegion::Cheongju => cheongju,
        TargetRegion::Gongju => gongju,
        TargetRegion::Boryeong => boryeong,
        TargetRegion::Asan => asan,
        TargetRegion::Cheonan => cheonan,
    };
    *region_count = region_count.saturating_add(1);
}
fn target_region_from_normalized(text: &str) -> Option<TargetRegion> {
    if text.strip_prefix("대전").is_some_and(|tail| {
        DAEJEON_DISTRICT_KEYS
            .iter()
            .any(|district| tail.starts_with(district))
    }) {
        return Some(TargetRegion::Daejeon);
    }
    if text.starts_with("세종") {
        return Some(TargetRegion::Sejong);
    }
    if text.starts_with("충북청주") || text.starts_with("청주") {
        return Some(TargetRegion::Cheongju);
    }
    if text.starts_with("충남공주") || text.starts_with("공주") {
        return Some(TargetRegion::Gongju);
    }
    if text.starts_with("충남보령") || text.starts_with("보령") {
        return Some(TargetRegion::Boryeong);
    }
    if text.starts_with("충남아산") || text.starts_with("아산") {
        return Some(TargetRegion::Asan);
    }
    if text.starts_with("충남천안") || text.starts_with("천안") {
        return Some(TargetRegion::Cheonan);
    }
    None
}
