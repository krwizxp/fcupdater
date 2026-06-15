use crate::diagnostic::{Result, err_with_source};
use alloc::string::String;
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
pub fn normalize_address_key(addr: &str) -> Result<String> {
    let mut out = String::new();
    normalize_address_key_into(addr, &mut out)?;
    Ok(out)
}
pub fn normalize_address_key_into(addr: &str, out: &mut String) -> Result<()> {
    let mut rest = addr.trim();
    let capacity = rest.len();
    out.clear();
    out.try_reserve(capacity)
        .map_err(|source| err_with_source("주소 key 정규화 메모리 확보 실패", source))?;
    while !rest.is_empty() {
        let mut replaced = false;
        if matches!(rest.chars().next(), Some('충' | '대' | '세')) {
            for rule in ADDRESS_KEY_REPLACEMENTS {
                let Some(tail) = rest.strip_prefix(rule.from) else {
                    continue;
                };
                out.push_str(rule.to);
                rest = tail;
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
        if ch.is_whitespace() {
            continue;
        }
        if matches!(ch, '(' | ')' | '[' | ']' | '{' | '}' | ',' | '.') {
            continue;
        }
        out.push(ch);
    }
    Ok(())
}
