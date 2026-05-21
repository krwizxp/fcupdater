use alloc::string::String;
const ADDRESS_KEY_REPLACEMENTS: [(&str, &str); 4] = [
    ("충청남도", "충남"),
    ("충청북도", "충북"),
    ("대전광역시", "대전"),
    ("세종특별자치시", "세종"),
];
const REGION_LABEL_SUFFIXES: [&str; 3] = ["특별자치시", "광역시", "특별시"];
pub fn normalize_address_key(addr: &str) -> String {
    let mut rest = addr.trim();
    let capacity = rest.len();
    let mut out = String::with_capacity(capacity);
    while !rest.is_empty() {
        if let Some((tail, to)) = ADDRESS_KEY_REPLACEMENTS
            .iter()
            .copied()
            .find_map(|(from, to)| rest.strip_prefix(from).map(|tail| (tail, to)))
        {
            out.push_str(to);
            rest = tail;
            continue;
        }
        let mut chars = rest.chars();
        let Some(ch) = chars.next() else {
            break;
        };
        rest = chars.as_str();
        if ch.is_whitespace() {
            continue;
        }
        if matches!(ch, '(' | ')' | '[' | ']' | '{' | '}' | ',' | '.') {
            continue;
        }
        out.push(ch);
    }
    out
}
pub fn parse_region_label(text: &str) -> Option<String> {
    let mut tokens = text.split_whitespace();
    let first = tokens.next()?;
    let second = tokens.next();
    for suffix in REGION_LABEL_SUFFIXES {
        if let Some(label) = first.strip_suffix(suffix)
            && !label.is_empty()
        {
            return Some(label.to_owned());
        }
    }
    if first.ends_with('도')
        || matches!(
            first,
            "충남" | "충북" | "경기" | "강원" | "전북" | "전남" | "경북" | "경남" | "제주"
        )
    {
        return second.map(|token| strip_basic_region_suffix(token).unwrap_or(token).to_owned());
    }
    if matches!(
        first,
        "서울" | "부산" | "대구" | "인천" | "광주" | "대전" | "울산" | "세종"
    ) {
        return Some(first.to_owned());
    }
    match strip_basic_region_suffix(first) {
        Some(label) => Some(label.to_owned()),
        None if second.is_none() => Some(first.to_owned()),
        None => None,
    }
}
fn strip_basic_region_suffix(token: &str) -> Option<&str> {
    token
        .strip_suffix(['시', '군', '구'])
        .filter(|label| !label.is_empty())
}
