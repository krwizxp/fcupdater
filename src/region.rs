use crate::diagnostic::{Result, err_fmt, reserve_err};
use std::process;
const CITY_REGIONS: [(TargetRegion, &str, &str); 5] = [
    (TargetRegion::Cheongju, "충북", "청주"),
    (TargetRegion::Gongju, "충남", "공주"),
    (TargetRegion::Boryeong, "충남", "보령"),
    (TargetRegion::Asan, "충남", "아산"),
    (TargetRegion::Cheonan, "충남", "천안"),
];
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
    pub(super) fn from_label(label: &str) -> Option<Self> {
        TARGET_REGIONS
            .into_iter()
            .find(|region| region.label() == label)
    }
    const fn index(self) -> usize {
        match self {
            Self::Daejeon => 0,
            Self::Sejong => 1,
            Self::Cheongju => 2,
            Self::Gongju => 3,
            Self::Boryeong => 4,
            Self::Asan => 5,
            Self::Cheonan => 6,
        }
    }
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
    pub(super) fn value<T>(self, values: &[T; TARGET_REGION_COUNT]) -> T
    where
        T: Copy,
    {
        *values.get(self.index()).unwrap_or_else(|| process::abort())
    }
    pub(super) fn value_mut<T>(self, values: &mut [T; TARGET_REGION_COUNT]) -> &mut T {
        values
            .get_mut(self.index())
            .unwrap_or_else(|| process::abort())
    }
}
pub(super) fn normalize_address_key_into(addr: &str, out: &mut String) -> Result<()> {
    let trimmed = addr.trim();
    out.clear();
    out.try_reserve(trimmed.len())
        .map_err(|source| reserve_err("주소 key 정규화 메모리 확보 실패", source))?;
    let mut run_start = 0_usize;
    for (idx, ch) in trimmed.char_indices() {
        let skip =
            ch.is_whitespace() || matches!(ch, '(' | ')' | '[' | ']' | '{' | '}' | ',' | '.');
        let check = matches!(ch, '도' | '시');
        if !skip && !check {
            continue;
        }
        let end = if skip {
            idx
        } else {
            idx.strict_add(ch.len_utf8())
        };
        if run_start < end {
            out.push_str(
                trimmed
                    .get(run_start..end)
                    .unwrap_or_else(|| process::abort()),
            );
        }
        run_start = idx.strict_add(ch.len_utf8());
        if check {
            for (from, to) in ADDRESS_KEY_REPLACEMENTS {
                if out.ends_with(from) {
                    out.truncate(out.len().strict_sub(from.len()));
                    out.push_str(to);
                    break;
                }
            }
        }
    }
    if run_start < trimmed.len() {
        out.push_str(trimmed.get(run_start..).unwrap_or_else(|| process::abort()));
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
        if address_region.is_none()
            && !region
                .trim_start_matches(|ch: char| {
                    ch.is_whitespace()
                        || matches!(ch, '(' | ')' | '[' | ']' | '{' | '}' | ',' | '.')
                })
                .starts_with(['대', '세', '충'])
        {
            return Ok(None);
        }
        normalize_address_key_into(region, scratch)?;
        let normalized_region = scratch.as_str();
        let parsed_region = if normalized_region == "대전"
            || normalized_region
                .strip_prefix("대전")
                .is_some_and(|district| DAEJEON_DISTRICT_KEYS.contains(&district))
        {
            Some(TargetRegion::Daejeon)
        } else if normalized_region == "세종시" {
            Some(TargetRegion::Sejong)
        } else {
            CITY_REGIONS
                .into_iter()
                .find_map(|(target, province, city)| {
                    (normalized_region
                        .strip_prefix(province)
                        .and_then(|tail| tail.strip_suffix("시"))
                        == Some(city))
                    .then_some(target)
                })
        };
        return match (parsed_region, address_region) {
            (Some(region_match), Some(address_match)) if region_match == address_match => {
                Ok(Some(address_match))
            }
            (Some(_), Some(_)) => Err(err_fmt(format_args!(
                "Opinet 소스의 지역 값과 주소가 서로 다른 대상 지역을 가리킵니다: region={region}, address={address}"
            ))),
            (Some(_), None) => Err(err_fmt(format_args!(
                "Opinet 소스의 지역 값은 대상 지역이지만 주소는 대상 지역이 아닙니다: region={region}, address={address}"
            ))),
            (None, Some(_)) => Err(err_fmt(format_args!(
                "Opinet 소스의 주소는 대상 지역이지만 지역 값이 예상 형식과 다릅니다: region={region}, address={address}"
            ))),
            (None, None) => Ok(None),
        };
    }
    let trimmed_region = region.trim();
    let (region_is_daejeon, region_match) = match TargetRegion::from_label(trimmed_region) {
        Some(TargetRegion::Daejeon) => (true, None),
        Some(target) => (false, Some(target)),
        None => {
            normalize_address_key_into(trimmed_region, scratch)?;
            let is_daejeon = scratch.as_str() == "대전";
            let matched = if is_daejeon {
                None
            } else {
                target_region_from_normalized(scratch.as_str())
            };
            (is_daejeon, matched)
        }
    };
    normalize_address_key_into(address, scratch)?;
    if let Some(target) = region_match.or_else(|| target_region_from_normalized(scratch.as_str())) {
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
pub(super) fn increment_target_region_count(
    counts: &mut [usize; TARGET_REGION_COUNT],
    region: TargetRegion,
) {
    let count = region.value_mut(counts);
    *count = count.strict_add(1);
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
    CITY_REGIONS
        .into_iter()
        .find_map(|(target, province, city)| {
            text.strip_prefix(province)
                .unwrap_or(text)
                .starts_with(city)
                .then_some(target)
        })
}
