pub const OPDOWNLOAD_PAGE_READY_SCRIPT: &str = r#"
return (function() {
  if (document.readyState !== "complete") return "";
  var bodyText = document.body ? String(document.body.innerText || document.body.textContent || "") : "";
  bodyText = bodyText.replace(/\s+/g, " ").trim();
  if (!bodyText) return "";
  if (!/(\uC0AC\uC5C5\uC790\uBCC4|\uD310\uB9E4\uAC00\uACA9|\uC5D1\uC140|\uB2E4\uC6B4\uB85C\uB4DC)/.test(bodyText)) return "";
  return "READY";
})();"#;
pub const OPDOWNLOAD_DISCOVERY_SCRIPT: &str = r#"
return (function() {
  function clean(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }
  function attr(el, name) {
    return clean(el && el.getAttribute ? el.getAttribute(name) : "");
  }
  function textOf(el) {
    if (!el) return "";
    return clean(el.innerText || el.textContent || el.value || attr(el, "aria-label") || attr(el, "title") || attr(el, "alt"));
  }
  function isVisible(el) {
    if (!el) return false;
    if (el.hidden) return false;
    var style = window.getComputedStyle ? window.getComputedStyle(el) : null;
    if (style && (style.display === "none" || style.visibility === "hidden")) return false;
    return !!(el.offsetWidth || el.offsetHeight || (el.getClientRects && el.getClientRects().length));
  }
  function contextOf(el) {
    var cur = el;
    while (cur && cur !== document.body) {
      var tag = cur.tagName ? cur.tagName.toLowerCase() : "";
      if (/^(tr|li|dd|dt|p|div|section|article|td|th|form)$/.test(tag)) {
        var text = clean(cur.innerText || cur.textContent || "");
        if (text && text.length <= 260) return text;
      }
      cur = cur.parentElement;
    }
    return "";
  }
  function pushLine(lines, el) {
    var text = textOf(el);
    var href = attr(el, "href");
    var onclick = attr(el, "onclick");
    var ctx = contextOf(el);
    var blob = [text, href, onclick, ctx].join(" ");
    if (!/(\uC0AC\uC5C5\uC790\uBCC4|\uD604\uC7AC|\uD310\uB9E4\uAC00\uACA9|\uC5D1\uC140|\uB2E4\uC6B4\uB85C\uB4DC|\uC800\uC7A5|excel|download|xls|xlsx)/i.test(blob)) return;
    lines.push([
      "el",
      (el.tagName || "").toLowerCase(),
      "id=" + attr(el, "id"),
      "name=" + attr(el, "name"),
      "type=" + attr(el, "type"),
      "text=" + text,
      "href=" + href,
      "onclick=" + onclick,
      "ctx=" + ctx
    ].join(" | "));
  }
  var lines = [];
  lines.push("title=" + clean(document.title));
  lines.push("url=" + clean(location.href));
  lines.push("body=" + clean(document.body ? (document.body.innerText || document.body.textContent || "") : "").slice(0, 400));
  if (typeof fn_Download === "function") {
    lines.push("fn_Download=" + clean(String(fn_Download)).slice(0, 2000));
  }
  var forms = Array.prototype.slice.call(document.forms || []);
  for (var f = 0; f < forms.length; f++) {
    var form = forms[f];
    lines.push([
      "form",
      "id=" + attr(form, "id"),
      "name=" + attr(form, "name"),
      "method=" + attr(form, "method"),
      "action=" + attr(form, "action"),
      "target=" + attr(form, "target")
    ].join(" | "));
    var inputs = Array.prototype.slice.call(form.querySelectorAll('input[type="hidden"],input[type="text"],input[type="radio"],select'));
    for (var p = 0; p < inputs.length; p++) {
      var input = inputs[p];
      lines.push([
        "field",
        "form=" + (attr(form, "id") || attr(form, "name")),
        "tag=" + (input.tagName || "").toLowerCase(),
        "type=" + attr(input, "type"),
        "name=" + attr(input, "name"),
        "id=" + attr(input, "id"),
        "value=" + clean(input.value || ""),
        "checked=" + (input.checked ? "Y" : "N")
      ].join(" | "));
    }
  }
  var all = Array.prototype.slice.call(document.querySelectorAll('a,button,input[type="button"],input[type="submit"],input[type="image"],*[onclick]'));
  for (var i = 0; i < all.length; i++) {
    if (!isVisible(all[i])) continue;
    pushLine(lines, all[i]);
  }
  return lines.join("\n");
})();"#;
pub const OPDOWNLOAD_TRIGGER_SCRIPT: &str = r#"
return (function() {
  function clean(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }
  function attr(el, name) {
    return clean(el && el.getAttribute ? el.getAttribute(name) : "");
  }
  function textOf(el) {
    if (!el) return "";
    return clean(el.innerText || el.textContent || el.value || attr(el, "aria-label") || attr(el, "title") || attr(el, "alt"));
  }
  function isVisible(el) {
    if (!el) return false;
    if (el.hidden) return false;
    var style = window.getComputedStyle ? window.getComputedStyle(el) : null;
    if (style && (style.display === "none" || style.visibility === "hidden")) return false;
    return !!(el.offsetWidth || el.offsetHeight || (el.getClientRects && el.getClientRects().length));
  }
  function collectClickables(root) {
    var items = [];
    if (root && root.matches && root.matches('a,button,input[type="button"],input[type="submit"],input[type="image"],*[onclick]')) {
      items.push(root);
    }
    if (root && root.querySelectorAll) {
      var descendants = root.querySelectorAll('a,button,input[type="button"],input[type="submit"],input[type="image"],*[onclick]');
      for (var i = 0; i < descendants.length; i++) items.push(descendants[i]);
    }
    return items.filter(isVisible);
  }
  function contextOf(el) {
    var cur = el;
    var fallback = "";
    while (cur && cur !== document.body) {
      var tag = cur.tagName ? cur.tagName.toLowerCase() : "";
      if (/^(tr|li|dd|dt|p|div|section|article|td|th|form)$/.test(tag)) {
        var text = clean(cur.innerText || cur.textContent || "");
        if (text && !fallback) fallback = text;
        if (text && text.length <= 320 && /(\uC0AC\uC5C5\uC790\uBCC4|\uD310\uB9E4\uAC00\uACA9|\uD604\uC7AC)/.test(text)) return text;
      }
      cur = cur.parentElement;
    }
    return fallback.slice(0, 320);
  }
  function score(blob) {
    var total = 0;
    if (/\uC0AC\uC5C5\uC790\uBCC4/.test(blob)) total += 25;
    if (/\uD604\uC7AC \uD310\uB9E4\uAC00\uACA9/.test(blob)) total += 25;
    if (/\uD310\uB9E4\uAC00\uACA9/.test(blob)) total += 16;
    if (/\uD604\uC7AC/.test(blob)) total += 4;
    if (/(\uC5D1\uC140|excel)/i.test(blob)) total += 14;
    if (/(\uB2E4\uC6B4\uB85C\uB4DC|\uC800\uC7A5)/.test(blob)) total += 10;
    if (/(download|xls|xlsx)/i.test(blob)) total += 8;
    return total;
  }
  function click(el) {
    try { el.scrollIntoView({ block: "center" }); } catch (e) {}
    if (typeof el.click === "function") {
      el.click();
      return;
    }
    var evt = document.createEvent("MouseEvents");
    evt.initMouseEvent("click", true, true, window, 1);
    el.dispatchEvent(evt);
  }
  if (typeof fn_Download === "function") {
    fn_Download(2);
    return "OK|fn_Download(2)|target=\uC0AC\uC5C5\uC790\uBCC4 \uD604\uC7AC \uD310\uB9E4\uAC00\uACA9 \uC5D1\uC140";
  }
  var direct = document.querySelector('a[href*="fn_Download(2)"]');
  if (direct && isVisible(direct)) {
    click(direct);
    return "OK|href=fn_Download(2)|target=\uC0AC\uC5C5\uC790\uBCC4 \uD604\uC7AC \uD310\uB9E4\uAC00\uACA9 \uC5D1\uC140";
  }
  var best = null;
  var containers = Array.prototype.slice.call(document.querySelectorAll("tr,li,dd,dt,p,div,section,article,td,th,form"));
  for (var i = 0; i < containers.length; i++) {
    var ctx = clean(containers[i].innerText || containers[i].textContent || "");
    if (!ctx || ctx.length > 320) continue;
    if (!/\uC0AC\uC5C5\uC790\uBCC4/.test(ctx) || !/(\uD604\uC7AC \uD310\uB9E4\uAC00\uACA9|\uD310\uB9E4\uAC00\uACA9)/.test(ctx)) continue;
    var clickables = collectClickables(containers[i]);
    for (var j = 0; j < clickables.length; j++) {
      var el = clickables[j];
      var blob = [ctx, textOf(el), attr(el, "href"), attr(el, "onclick"), attr(el, "title")].join(" ");
      var candidate = {
        el: el,
        score: score(blob),
        text: textOf(el),
        href: attr(el, "href"),
        onclick: attr(el, "onclick"),
        ctx: ctx,
        tag: (el.tagName || "").toLowerCase()
      };
      if (!best || candidate.score > best.score || (candidate.score === best.score && candidate.ctx.length < best.ctx.length)) {
        best = candidate;
      }
    }
  }
  if (!best) {
    var all = collectClickables(document);
    for (var k = 0; k < all.length; k++) {
      var item = all[k];
      var ctx2 = contextOf(item);
      var blob2 = [ctx2, textOf(item), attr(item, "href"), attr(item, "onclick"), attr(item, "title")].join(" ");
      if (!/\uC0AC\uC5C5\uC790\uBCC4/.test(blob2) || !/(\uD604\uC7AC \uD310\uB9E4\uAC00\uACA9|\uD310\uB9E4\uAC00\uACA9)/.test(blob2)) continue;
      if (!/(\uC5D1\uC140|\uB2E4\uC6B4\uB85C\uB4DC|\uC800\uC7A5|excel|download|xls|xlsx)/i.test(blob2)) continue;
      var fallback = {
        el: item,
        score: score(blob2),
        text: textOf(item),
        href: attr(item, "href"),
        onclick: attr(item, "onclick"),
        ctx: ctx2,
        tag: (item.tagName || "").toLowerCase()
      };
      if (!best || fallback.score > best.score) best = fallback;
    }
  }
  if (!best || best.score < 25) {
    return "ERR:NO_TARGET";
  }
  click(best.el);
  return [
    "OK",
    "tag=" + best.tag,
    "score=" + String(best.score),
    "text=" + best.text,
    "href=" + best.href,
    "onclick=" + best.onclick,
    "ctx=" + best.ctx
  ].join("|");
})();"#;
pub const OPDOWNLOAD_DIAGNOSTIC_SCRIPT: &str = r#"
return (function() {
  function clean(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }
  return [
    "title=" + clean(document.title),
    "url=" + clean(location.href),
    "ready=" + clean(document.readyState),
    "body=" + clean(document.body ? (document.body.innerText || document.body.textContent || "") : "").slice(0, 500)
  ].join(" | ");
})();"#;
