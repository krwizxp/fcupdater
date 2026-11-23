use crate::diagnostic::{Result, fmt_err, or_abort, reserve};
const ADDR_REPL: [(&str, &str); 4] = [
    ("충청남도", "충남"),
    ("충청북도", "충북"),
    ("대전광역시", "대전"),
    ("세종특별자치시", "세종"),
];
const CITY_REGIONS: [(TargetRegion, &str, &str); 5] = [
    (TargetRegion::Cheongju, "충북", "청주"),
    (TargetRegion::Gongju, "충남", "공주"),
    (TargetRegion::Boryeong, "충남", "보령"),
    (TargetRegion::Asan, "충남", "아산"),
    (TargetRegion::Cheonan, "충남", "천안"),
];
const DJ_DISTRICTS: [&str; 5] = ["대덕구", "동구", "서구", "유성구", "중구"];
pub(super) const TARGET_REGION_COUNT: usize = 7;
pub(super) const TARGET_REGIONS: [TargetRegion; TARGET_REGION_COUNT] = [
    TargetRegion::Daejeon,
    TargetRegion::Sejong,
    TargetRegion::Cheongju,
    TargetRegion::Gongju,
    TargetRegion::Boryeong,
    TargetRegion::Asan,
    TargetRegion::Cheonan,
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
        TARGET_REGIONS.into_iter().find(|reg| reg.label() == label)
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
        *or_abort(values.get(self.index()))
    }
    pub(super) fn value_mut<T>(self, values: &mut [T; TARGET_REGION_COUNT]) -> &mut T {
        or_abort(values.get_mut(self.index()))
    }
}
fn daejeon_region(district: &str) -> Option<TargetRegion> {
    DJ_DISTRICTS
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
pub(super) fn normalize_address_key_into(addr: &str, out: &mut String) -> Result<()> {
    let trimmed = addr.trim();
    out.clear();
    reserve(out.try_reserve(trimmed.len()), "주소 key 정규화")?;
    let mut run_start = 0_usize;
    for (idx, ch) in trimmed.char_indices() {
        let skip =
            ch.is_whitespace() || matches!(ch, '(' | ')' | '[' | ']' | '{' | '}' | ',' | '.');
        let check = matches!(ch, '도' | '시');
        if !skip && !check {
            continue;
        }
        let next = idx.strict_add(ch.len_utf8());
        let end = if skip { idx } else { next };
        if run_start < end {
            out.push_str(or_abort(trimmed.get(run_start..end)));
        }
        run_start = next;
        if check {
            for (from, to) in ADDR_REPL {
                if out.ends_with(from) {
                    out.truncate(out.len().strict_sub(from.len()));
                    out.push_str(to);
                    break;
                }
            }
        }
    }
    if run_start < trimmed.len() {
        out.push_str(or_abort(trimmed.get(run_start..)));
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
        let addr_reg = match tokens.next() {
            Some("대전") => match tokens.next() {
                Some("광역시") => tokens.next().and_then(daejeon_region),
                dist => dist.and_then(daejeon_region),
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
        if addr_reg.is_none()
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
        let norm = scratch.as_str();
        let parsed = if norm == "대전"
            || norm
                .strip_prefix("대전")
                .is_some_and(|dist| DJ_DISTRICTS.contains(&dist))
        {
            Some(TargetRegion::Daejeon)
        } else if norm == "세종시" {
            Some(TargetRegion::Sejong)
        } else {
            CITY_REGIONS.into_iter().find_map(|(target, prov, city)| {
                (norm
                    .strip_prefix(prov)
                    .and_then(|tail| tail.strip_suffix("시"))
                    == Some(city))
                .then_some(target)
            })
        };
        return match (parsed, addr_reg) {
            (Some(rm), Some(am)) if rm == am => Ok(Some(am)),
            (Some(_), Some(_)) => Err(fmt_err!(
                "지역과 주소가 서로 다른 대상 지역: region={region}, address={address}"
            )),
            (Some(_), None) => Err(fmt_err!(
                "지역은 대상이지만 주소가 아님: region={region}, address={address}"
            )),
            (None, Some(_)) => Err(fmt_err!(
                "주소는 대상이지만 지역 형식 오류: region={region}, address={address}"
            )),
            (None, None) => Ok(None),
        };
    }
    let trimmed = region.trim();
    let (is_dj, reg_match) = match TargetRegion::from_label(trimmed) {
        Some(TargetRegion::Daejeon) => (true, None),
        Some(target) => (false, Some(target)),
        None => {
            normalize_address_key_into(trimmed, scratch)?;
            let dj = scratch.as_str() == "대전";
            (
                dj,
                (!dj).then(|| region_from_norm(scratch.as_str())).flatten(),
            )
        }
    };
    normalize_address_key_into(address, scratch)?;
    if let Some(target) = reg_match.or_else(|| region_from_norm(scratch.as_str())) {
        return Ok(Some(target));
    }
    Ok(is_dj
        .then(|| {
            DJ_DISTRICTS
                .iter()
                .any(|dist| scratch.starts_with(dist))
                .then_some(TargetRegion::Daejeon)
        })
        .flatten())
}
fn region_from_norm(text: &str) -> Option<TargetRegion> {
    if text
        .strip_prefix("대전")
        .is_some_and(|tail| DJ_DISTRICTS.iter().any(|dist| tail.starts_with(dist)))
    {
        return Some(TargetRegion::Daejeon);
    }
    if text.starts_with("세종") {
        return Some(TargetRegion::Sejong);
    }
    CITY_REGIONS.into_iter().find_map(|(target, prov, city)| {
        text.strip_prefix(prov)
            .unwrap_or(text)
            .starts_with(city)
            .then_some(target)
    })
}
