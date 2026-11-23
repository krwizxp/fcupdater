use super::{
    CHANGE_LOG_SHEET_NAME, CHANGE_LOG_SHEET_PATH, CanonicalStyleMap, FILTER_DATABASE_REF_PREFIX,
    MASTER_SHEET_NAME, MASTER_SHEET_PATH, SPREADSHEETML_NAMESPACE, SaveVerification,
    canonical_excel_fragment, copy_text,
    xlsx_container::{XlsxContainer, validate_spreadsheet_xml_document},
    xml::{XmlAttrScanner, XmlScanner, decode_xml_entities, extract_first_tag_text},
};
use crate::{
    diagnostic::{
        AppError, Result, append_fmt, bail_fmt, ensure, ensure_fmt, fmt_err, or_abort,
        parse_unsigned_u32, push_vec, reserve, str_cap, vec_cap,
    },
    master_sheet::{DECIMAL_SCALE, MasterSheetUpdater},
};
use alloc::{borrow::Cow, rc::Rc};
use core::{
    cmp::Ordering,
    fmt::NumBuffer,
    iter::repeat_n,
    mem,
    range::{Range, RangeInclusive},
    result::Result as CoreResult,
};
use std::{collections::HashMap, path::Path, process};
macro_rules! push_dec {
    ($out:expr, $val:expr) => {
        ($out).push_str(($val).format_into(&mut NumBuffer::new()));
    };
}
const MAX_A1_ROW: u32 = 0x0010_0000;
const SINGLE_COLS: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const MAX_SST: usize = 0x0010_0000;
const MAX_CELLS: usize = 0x0010_0000;
const MAX_SF_FOLLOW: u32 = 63;
const MAX_SF_GAP: u32 = 31;
const MIN_SF_CELLS: u32 = 6;
const CL_LAST_COL: u32 = 13;
const MS_LAST_COL: u32 = 23;
const SST_GROW: usize = 16;
const MS_PREFIX: &str = include_str!("excel_sheet1_prefix.xml");
const MS_SUFFIX: &str = include_str!("excel_sheet1_suffix.xml");
const CL_PREFIX: &str = include_str!("excel_sheet2_prefix.xml");
const CL_SUFFIX: &str = include_str!("excel_sheet2_suffix.xml");
const CALC_EXT_XML: &str = concat!(
    "<extLst>",
    "<ext uri=\"{B58B0392-4F1F-4190-BB64-5DF3571DCE5F}\" xmlns:xcalcf=\"http://schemas.microsoft.com/office/spreadsheetml/2018/calcfeatures\"><xcalcf:calcFeatures><xcalcf:feature name=\"microsoft.com:RD\"/><xcalcf:feature name=\"microsoft.com:Single\"/><xcalcf:feature name=\"microsoft.com:FV\"/><xcalcf:feature name=\"microsoft.com:CNMTM\"/><xcalcf:feature name=\"microsoft.com:LET_WF\"/><xcalcf:feature name=\"microsoft.com:LAMBDA_WF\"/><xcalcf:feature name=\"microsoft.com:ARRAYTEXT_WF\"/></xcalcf:calcFeatures></ext>",
    "<ext uri=\"{D14903EA-33C4-47F7-8F05-3474C54BE107}\" xmlns:xlwcv=\"http://schemas.microsoft.com/office/spreadsheetml/2024/workbookCompatibilityVersion\"><xlwcv:version setVersion=\"1\"/></ext>",
    "<ext uri=\"{7626C862-2A13-11E5-B345-FEFF819CDC9F}\" xmlns:loext=\"http://schemas.libreoffice.org/\"><loext:extCalcPr stringRefSyntax=\"CalcA1ExcelA1\"/></ext>",
    "</extLst>",
);
const WB_OPEN_XML: &str = concat!(
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
    "<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\" xmlns:mc=\"http://schemas.openxmlformats.org/markup-compatibility/2006\" mc:Ignorable=\"x15 xr xr6 xr10 xr2\" xmlns:x15=\"http://schemas.microsoft.com/office/spreadsheetml/2010/11/main\" xmlns:xr=\"http://schemas.microsoft.com/office/spreadsheetml/2014/revision\" xmlns:xr6=\"http://schemas.microsoft.com/office/spreadsheetml/2016/revision6\" xmlns:xr10=\"http://schemas.microsoft.com/office/spreadsheetml/2016/revision10\" xmlns:xr2=\"http://schemas.microsoft.com/office/spreadsheetml/2015/revision2\">",
);
const MS_HEADERS: &str = "지역화폐적용순위|지역|상호|상표|셀프|주소|휘발유단가(원/L)|고급유단가(원/L)|울트라카젠 여부|경유단가(원/L)|스마트주유 할인(원/L)|조정휘발유단가(원/L)|조정고급유단가(원/L)|조정경유단가(원/L)|유종별 총가격(원)|총가격(원)|지역화폐 적용여부|지역화폐적립율|지역화폐적립액(원)|지역화폐적용금액(원)|지역화폐 적용단가(원/L)|지역화폐 미적용 단가(원/L)|정렬키";
const CL_HEADERS: &str = "지역|상호|주소|변경내용|휘발유(이전)|휘발유(신규)|휘발유 Δ|고급유(이전)|고급유(신규)|고급유 Δ|경유(이전)|경유(신규)|경유 Δ";
const CL_LAYOUT: FormulaLayout = FormulaLayout {
    data_start_row: 4,
    fixed_formulas: &[],
    optional_zero_col: None,
    required_cols: &[7, 10, CL_LAST_COL],
};
const MS_LAYOUT: FormulaLayout = FormulaLayout {
    data_start_row: 15,
    fixed_formulas: &[
        (2, 10, "B4+B5+B6"),
        (2, 11, r#"IF(B4+B5=0,"",(B4*B7+B5*B8)/(B4+B5))"#),
        (2, 12, r#"IF(B4+B5=0,"",(B4*B7+B5*B9)/(B4+B5))"#),
    ],
    optional_zero_col: Some(11),
    required_cols: &[1, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, MS_LAST_COL],
};
pub(crate) struct Workbook {
    change_log_sheet: Worksheet,
    container: XlsxContainer,
    master_sheet: Worksheet,
    shared_strings: SharedStringTable,
}
pub(crate) struct SharedStringTable {
    entries: Vec<SharedStringEntry>,
    index: HashMap<Rc<str>, usize>,
}
struct SharedStringEntry {
    text: Rc<str>,
    xml: Option<String>,
}
pub(crate) struct Worksheet {
    rows: Vec<Row>,
}
#[derive(Default)]
pub(crate) struct Row {
    attrs_xml: String,
    cells: Vec<Cell>,
}
struct Cell {
    col: u32,
    inner_xml: String,
    style: Option<u32>,
    value_type: CellType,
}
struct SharedFormulaHead {
    anchor_col: u32,
    anchor_row: u32,
    last_row: u32,
    seen: u32,
}
#[derive(Clone, Copy)]
struct FormulaLayout {
    data_start_row: u32,
    fixed_formulas: &'static [(u32, u32, &'static str)],
    optional_zero_col: Option<u32>,
    required_cols: &'static [u32],
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Sheet {
    ChangeLog,
    Master,
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum CellType {
    Gen,
    Sst(usize),
    Str,
}
struct WorksheetParser<'styles> {
    cell_count: usize,
    dense_formula_heads: Vec<SharedFormulaHead>,
    input_styles: &'styles CanonicalStyleMap,
    sheet: Sheet,
    sparse_formula_heads: HashMap<u32, SharedFormulaHead>,
}
impl SharedStringTable {
    fn get(&self, index: usize) -> Option<&str> {
        self.entries.get(index).map(|entry| entry.text.as_ref())
    }
    fn intern(&mut self, value: &str) -> Result<usize> {
        if self.index.is_empty() && !self.entries.is_empty() {
            reserve(self.index.try_reserve(self.entries.len()), "SST index")?;
            for (idx, entry) in self.entries.iter().enumerate() {
                self.index.entry(Rc::clone(&entry.text)).or_insert(idx);
            }
        }
        if let Some(&idx) = self.index.get(value) {
            return Ok(idx);
        }
        ensure_fmt!(self.entries.len() < MAX_SST, "SST 개수 한도 초과");
        let growth = SST_GROW.min(MAX_SST.strict_sub(self.entries.len()));
        if self.index.len() == self.index.capacity() {
            reserve(self.index.try_reserve(growth), "SST index")?;
        }
        if self.entries.len() == self.entries.capacity() {
            reserve(self.entries.try_reserve(growth), "SST entry")?;
        }
        let idx = self.entries.len();
        let stored = Rc::<str>::from(value);
        xml_escaped_len(value, false, "SST")?;
        self.entries.push(SharedStringEntry {
            text: Rc::clone(&stored),
            xml: None,
        });
        self.index.insert(stored, idx);
        Ok(idx)
    }
    pub(crate) fn set_cell(
        &mut self,
        ws: &mut Worksheet,
        col: u32,
        row: u32,
        val: &str,
    ) -> Result<()> {
        let idx = self.intern(val)?;
        let cell = Worksheet::get_or_create_cell_mut(&mut ws.rows, col, row)?;
        cell.value_type = CellType::Sst(idx);
        cell.inner_xml.clear();
        Ok(())
    }
    fn to_xml(&self, ref_count: usize) -> Result<String> {
        let cap = self.entries.iter().fold(262_usize, |sum, en| {
            sum.strict_add(
                en.xml
                    .as_ref()
                    .map_or_else(|| en.text.len().strict_add(48), String::len),
            )
        });
        let mut xml = str_cap(cap, "SST XML")?;
        append_fmt(
            &mut xml,
            format_args!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n<sst xmlns=\"{SPREADSHEETML_NAMESPACE}\" count=\"{ref_count}\" uniqueCount=\"{}\">",
                self.entries.len()
            ),
        );
        for en in &self.entries {
            if let Some(orig) = en.xml.as_ref() {
                reserve_str(&mut xml, orig.len(), "SST XML")?;
                xml.push_str(orig);
            } else {
                let open = if has_edge_ws(&en.text) {
                    "<si><t xml:space=\"preserve\">"
                } else {
                    "<si><t>"
                };
                let worst = open
                    .len()
                    .strict_add(en.text.len().strict_mul(5))
                    .strict_add(9);
                if xml.capacity().strict_sub(xml.len()) < worst {
                    reserve_str(&mut xml, worst, "SST XML")?;
                }
                xml.push_str(open);
                push_xml_esc(&mut xml, &en.text, false);
                xml.push_str("</t></si>");
            }
        }
        xml.push_str("</sst>");
        Ok(xml)
    }
}
impl Workbook {
    pub(crate) const fn change_log_sheet_mut(
        &mut self,
    ) -> (&mut Worksheet, &mut SharedStringTable) {
        (&mut self.change_log_sheet, &mut self.shared_strings)
    }
    pub(crate) fn from_container(mut container: XlsxContainer) -> Result<Self> {
        container.ensure_supported_workbook()?;
        let master_xml = container.take_worksheet_text(MASTER_SHEET_PATH)?;
        let cl_xml = container.take_worksheet_text(CHANGE_LOG_SHEET_PATH)?;
        let styles = container.package_prepare_excel_output()?;
        let sst_xml = container.take_shared_strings_text()?;
        let mut sc = XmlScanner::new(&sst_xml);
        let sst = sc
            .next_start_named("sst")
            .ok_or("sharedStrings XML에 <sst> 없음")?;
        let mut entries = vec_cap(4096, "SST entry")?;
        let sst_close = (!sst.self_closing).then_some(sst.name);
        let mut attrs: Vec<(&str, Cow<'_, str>)> = Vec::new();
        while let Some(close_name) = sst_close
            && let Some(si) = sc.next_direct_element_named_until("si", close_name, 1, "sst")?
        {
            ensure_fmt!(entries.len() < MAX_SST, "SST 개수 한도 초과");
            if entries.len() == entries.capacity() {
                reserve(entries.try_reserve(SST_GROW), "SST entry")?;
            }
            let si_xml = or_abort(sst_xml.get(si.span));
            if let Some(body) = si_xml
                .strip_prefix("<si><t>")
                .or_else(|| si_xml.strip_prefix("<si><t xml:space=\"preserve\">"))
                .and_then(|tail| tail.strip_suffix("</t></si>"))
                .filter(|body| !body.contains('<'))
            {
                let plain = decode_xml_entities(body)?;
                entries.push(SharedStringEntry {
                    text: Rc::<str>::from(plain.as_ref()),
                    xml: None,
                });
                continue;
            }
            let mut xml = str_cap(si_xml.len(), "SST XML")?;
            let (mut t_sc, mut cur, mut val_text) =
                (XmlScanner::new(si_xml), 0_usize, None::<Cow<'_, str>>);
            while let Some(el) = t_sc.next_element_named("t")? {
                xml.push_str(or_abort(si_xml.get(cur..el.span.start)));
                attrs.clear();
                let mut a_sc = XmlAttrScanner::from_tag(&el.opening);
                while let Some((nm, val)) = a_sc.next()? {
                    ensure(
                        attrs.len() < 128 && !attrs.iter().any(|&(ex, _)| ex == nm),
                        "XML 속성 오류",
                    )?;
                    push_vec(&mut attrs, (nm, val), 1, "XML 속성")?;
                }
                let dec = (!el.opening.self_closing)
                    .then(|| decode_xml_entities(el.body))
                    .transpose()?;
                if let Some(tx) = dec.as_deref() {
                    attrs.retain(|&(nm, _)| nm != "xml:space");
                    if has_edge_ws(tx) {
                        push_vec(
                            &mut attrs,
                            ("xml:space", Cow::Borrowed("preserve")),
                            1,
                            "XML 속성",
                        )?;
                    }
                }
                xml.push_str("<t");
                for attr in &attrs {
                    let esc = xml_escape(&attr.1, true, "XML 태그")?;
                    reserve_str(
                        &mut xml,
                        attr.0.len().strict_add(esc.len()).strict_add(4),
                        "XML 태그",
                    )?;
                    xml.extend([" ", attr.0, "=\"", &esc, "\""]);
                }
                xml.push_str(if el.opening.self_closing { "/>" } else { ">" });
                if let Some(dec_tx) = dec {
                    let esc = xml_escape(&dec_tx, false, "SST XML")?;
                    reserve_str(&mut xml, esc.len().strict_add(4), "SST XML")?;
                    xml.extend([esc.as_ref(), "</t>"]);
                    if !dec_tx.is_empty() {
                        if let Some(acc) = val_text.as_mut() {
                            let owned = acc.to_mut();
                            reserve(owned.try_reserve_exact(dec_tx.len()), "XML tag text")?;
                            owned.push_str(&dec_tx);
                        } else {
                            val_text = Some(dec_tx);
                        }
                    }
                }
                cur = el.span.end;
            }
            xml.push_str(or_abort(si_xml.get(cur..)));
            entries.push(SharedStringEntry {
                text: Rc::<str>::from(val_text.as_deref().unwrap_or("")),
                xml: Some(xml),
            });
        }
        let shared_strings = SharedStringTable {
            entries,
            index: HashMap::new(),
        };
        let load = |kind, xml| -> Result<Worksheet> {
            let ws = WorksheetParser {
                cell_count: 0,
                dense_formula_heads: Vec::new(),
                sparse_formula_heads: HashMap::new(),
                input_styles: &styles,
                sheet: kind,
            }
            .parse(xml)?;
            ws.validate_fixed_header(kind, &shared_strings)?;
            ws.semantic_validation(kind, &shared_strings)?;
            Ok(ws)
        };
        let (master_sheet, change_log_sheet) = (
            load(Sheet::Master, &master_xml)?,
            load(Sheet::ChangeLog, &cl_xml)?,
        );
        Ok(Self {
            change_log_sheet,
            container,
            master_sheet,
            shared_strings,
        })
    }
    pub(crate) const fn master_sheet_mut(&mut self) -> (&mut Worksheet, &mut SharedStringTable) {
        (&mut self.master_sheet, &mut self.shared_strings)
    }
    fn request_recalculation_caches(&mut self) -> Result<()> {
        let mut input = [0_i128; 6];
        for (slot, row) in input.iter_mut().zip(4_u32..=9_u32) {
            *slot = i128::from(
                MasterSheetUpdater::get_f64_at(&self.master_sheet, 2, row, &self.shared_strings)?
                    .ok_or_else(|| fmt_err!("고정 입력값 비어 있음: B{row}"))?,
            );
        }
        let [gas_q, prem_q, dsl_q, gas_w, prem_w, dsl_w] = input;
        let mut cache = String::new();
        format_excel_ratio_into(
            &mut cache,
            gas_q.strict_add(prem_q).strict_add(dsl_q),
            i128::from(DECIMAL_SCALE),
        );
        self.master_sheet
            .set_formula_cached_value_at(2, 10, Some(&cache))?;
        let denom = gas_q.strict_add(prem_q);
        let gas_term = (denom != 0)
            .then(|| gas_q.checked_mul(gas_w).ok_or("혼합 옥탄가 overflow"))
            .transpose()?;
        for (row, weight) in [(11, prem_w), (12, dsl_w)] {
            let cached = gas_term
                .map(|gt| -> Result<&str> {
                    let num = prem_q
                        .checked_mul(weight)
                        .and_then(|pr| gt.checked_add(pr))
                        .ok_or("혼합 옥탄가 overflow")?;
                    format_excel_ratio_into(
                        &mut cache,
                        num,
                        denom.strict_mul(i128::from(DECIMAL_SCALE)),
                    );
                    Ok(cache.as_str())
                })
                .transpose()?;
            self.master_sheet
                .set_formula_cached_value_at(2, row, cached)?;
        }
        Ok(())
    }
    pub(crate) fn save(
        mut self,
        target_path: &Path,
        verification: SaveVerification,
        filter_last_row: u32,
        cl_last_row: u32,
    ) -> Result<()> {
        self.update_shared_string_catalog()?;
        self.request_recalculation_caches()?;
        self.master_sheet.canonical_share_formulas(Sheet::Master)?;
        self.change_log_sheet
            .canonical_share_formulas(Sheet::ChangeLog)?;
        let mut shared_count = 0_usize;
        for (ws, kind, path, last) in [
            (
                &mut self.master_sheet,
                Sheet::Master,
                MASTER_SHEET_PATH,
                filter_last_row,
            ),
            (
                &mut self.change_log_sheet,
                Sheet::ChangeLog,
                CHANGE_LOG_SHEET_PATH,
                cl_last_row,
            ),
        ] {
            ws.validate_fixed_header(kind, &self.shared_strings)?;
            let (xml, cnt) = ws.to_xml(kind, last)?;
            self.container.put_text(path, xml);
            drop(ws.take_rows());
            shared_count = shared_count.strict_add(cnt);
        }
        let sst_xml = self.shared_strings.to_xml(shared_count)?;
        let cap = WB_OPEN_XML
            .len()
            .strict_add(CALC_EXT_XML.len())
            .strict_add(FILTER_DATABASE_REF_PREFIX.len())
            .strict_add(550);
        let mut wb = str_cap(cap, "workbook XML")?;
        wb.extend([
            WB_OPEN_XML,
            "<workbookPr/><bookViews><workbookView/></bookViews>",
            "<sheets><sheet name=\"유류비\" sheetId=\"1\" r:id=\"rId1\"/><sheet name=\"변경내역\" sheetId=\"2\" r:id=\"rId2\"/></sheets><definedNames><definedName name=\"_xlnm._FilterDatabase\" localSheetId=\"0\" hidden=\"1\">",
            FILTER_DATABASE_REF_PREFIX,
        ]);
        push_dec!(&mut wb, filter_last_row);
        wb.extend([
            "</definedName></definedNames><calcPr/>",
            CALC_EXT_XML,
            "</workbook>",
        ]);
        self.container.put_text("xl/workbook.xml", wb);
        self.container.put_text("xl/sharedStrings.xml", sst_xml);
        self.container.save(target_path, verification)
    }
    fn update_shared_string_catalog(&mut self) -> Result<()> {
        let count = self.shared_strings.entries.len();
        self.shared_strings.index = HashMap::new();
        let mut map = vec_cap(count, "SST 변환표")?;
        map.resize(count, usize::MAX);
        for ws in [&self.master_sheet, &self.change_log_sheet] {
            for cell in ws.rows.iter().flat_map(|row| &row.cells) {
                if let CellType::Sst(idx) = cell.value_type {
                    *or_abort(map.get_mut(idx)) = 0;
                }
            }
        }
        let entries = &mut self.shared_strings.entries;
        let mut write = 0_usize;
        for (old, slot) in map.iter_mut().enumerate() {
            if *slot != usize::MAX {
                entries.swap(write, old);
                *slot = write;
                write = write.strict_add(1);
            }
        }
        entries.truncate(write);
        for ws in [&mut self.master_sheet, &mut self.change_log_sheet] {
            for cell in ws.rows.iter_mut().flat_map(|row| &mut row.cells) {
                if let CellType::Sst(old) = cell.value_type {
                    cell.value_type = CellType::Sst(or_abort(
                        map.get(old).copied().filter(|&idx| idx != usize::MAX),
                    ));
                }
            }
        }
        Ok(())
    }
}
impl WorksheetParser<'_> {
    fn parse(mut self, xml: &str) -> Result<Worksheet> {
        let mut sc = XmlScanner::new(xml);
        let sd = sc
            .next_start_named("sheetData")
            .ok_or("worksheet <sheetData> 없음")?;
        ensure(!sd.self_closing, "sheetData 비어 있음")?;
        let rows = self.scan_rows(&mut sc, sd.name)?;
        let span = sd.start..sc.cursor();
        let ctx = match self.sheet {
            Sheet::ChangeLog => "worksheet: 변경내역",
            Sheet::Master => "worksheet: 유류비",
        };
        validate_spreadsheet_xml_document(xml, "worksheet", ctx, false, Some(&span))?;
        let invalid = self
            .dense_formula_heads
            .into_iter()
            .zip(0_u32..)
            .map(|(head, id)| (id, head))
            .chain(self.sparse_formula_heads)
            .filter_map(|(si, hd)| {
                let exp = hd.last_row.strict_sub(hd.anchor_row).strict_add(1);
                (hd.seen != exp).then_some((si, exp, hd.seen))
            })
            .min_by_key(|&(si, ..)| si);
        if let Some((si, exp, act)) = invalid {
            bail_fmt!("shared formula ref 수 불일치: si={si}, expected={exp}, actual={act}");
        }
        Ok(Worksheet { rows })
    }
    fn parse_row(
        &mut self,
        sc: &mut XmlScanner<'_>,
        row_name: &str,
        row_num: u32,
        row: &mut Row,
    ) -> Result<()> {
        let mut next_col = 1_u32;
        while let Some(cell) =
            sc.next_direct_element_named_until("c", row_name, 2, "worksheet row")?
        {
            let (body, info) = (cell.body, cell.opening);
            let (mut ref_val, mut style_val, mut type_val) = (None, None, None);
            let mut attributes = XmlAttrScanner::from_tag(&info);
            while let Some((name, value)) = attributes.next()? {
                let slot = match name {
                    "r" => &mut ref_val,
                    "s" => &mut style_val,
                    "t" => &mut type_val,
                    _ => bail_fmt!("cell 알 수 없는 {name} 속성"),
                };
                ensure_fmt!(slot.replace(value).is_none(), "cell {name} 속성 중복");
            }
            let ref_tx = ref_val
                .as_deref()
                .ok_or_else(|| fmt_err!("cell ref 없음: row={row_num}"))?;
            let (col, ref_row, _) =
                parse_ref(ref_tx).ok_or_else(|| fmt_err!("cell ref 오류: {ref_tx}"))?;
            ensure_fmt!(
                ref_row == row_num && col >= next_col,
                "cell 위치/순서 오류: row={row_num}, col={col}"
            );
            let omit_note =
                self.sheet == Sheet::Master && row_num == 2 && col != 1 && info.self_closing;
            let style = (!omit_note)
                .then_some(style_val)
                .flatten()
                .map(|val| {
                    canon_style(
                        parse_u32(&val, "cell style")?,
                        self.sheet,
                        row_num,
                        Some(col),
                        self.input_styles,
                    )
                })
                .transpose()?;
            let (mut value_type, is_shared) = match type_val.as_deref() {
                None | Some("n") => (CellType::Gen, false),
                Some("s") => (CellType::Gen, true),
                Some("str") => (CellType::Str, false),
                Some(other) => bail_fmt!("미지원 cell type: {other}"),
            };
            let has_attrs = style.is_some() || value_type != CellType::Gen;
            let retain = if info.self_closing {
                ensure(!is_shared, "shared string cell 본문 없음")?;
                has_attrs.then(String::new)
            } else if is_shared {
                let val_body = if let Some(simple) = simple_v(body) {
                    simple
                } else {
                    let mut v_sc = XmlScanner::new(body);
                    let val = v_sc
                        .next_element_named("v")?
                        .filter(|el| el.opening.name == "v" && !el.opening.self_closing)
                        .ok_or("shared string cell에 v 태그 없음")?;
                    ensure(
                        XmlAttrScanner::from_tag(&val.opening).next()?.is_none()
                            && v_sc.next_tag().is_none()
                            && body
                                .get(..val.span.start)
                                .is_some_and(|tx| tx.trim().is_empty())
                            && body
                                .get(val.span.end..)
                                .is_some_and(|tx| tx.trim().is_empty()),
                        "shared string cell 본문 오류",
                    )?;
                    val.body
                };
                value_type = CellType::Sst(parse_u32(val_body, "SST index")? as usize);
                Some(String::new())
            } else if has_attrs || !body.is_empty() {
                Some(copy_text(body)?)
            } else {
                None
            };
            if let Some(inner_xml) = retain {
                self.retain_cell(
                    row_num,
                    row,
                    Cell {
                        col,
                        inner_xml,
                        style,
                        value_type,
                    },
                )?;
            }
            next_col = col.strict_add(1);
        }
        Ok(())
    }
    fn retain_cell(&mut self, row_num: u32, row: &mut Row, mut cell: Cell) -> Result<()> {
        let col = cell.col;
        ensure_fmt!(self.cell_count < MAX_CELLS, "worksheet cell 한도 초과");
        if !cell.inner_xml.is_empty() && simple_v(&cell.inner_xml).is_none() {
            let inner = cell.inner_xml.as_str();
            let mut f_sc = XmlScanner::new(inner);
            let (mut formula_el, mut has_cache) = (None, false);
            while let Some(child) = f_sc.next_direct_element("worksheet cell")? {
                if child.opening.raw.contains("xmlns") {
                    let mut attrs = XmlAttrScanner::from_tag(&child.opening);
                    while let Some((nm, _)) = attrs.next()? {
                        ensure_fmt!(
                            nm != "xmlns" && !nm.starts_with("xmlns:"),
                            "cell namespace 재정의 미지원"
                        );
                    }
                }
                ensure_fmt!(
                    !child.body.contains('<'),
                    "cell 자식 중첩 요소: row={row_num}, col={col}"
                );
                match child.opening.name {
                    "f" if formula_el.is_none() && !has_cache => formula_el = Some(child),
                    "v" if !has_cache => has_cache = true,
                    _ => bail_fmt!("cell 자식 오류: row={row_num}, col={col}"),
                }
            }
            if let Some(formula) = formula_el {
                let decoded = decode_xml_entities(formula.body)?;
                let (mut aca, mut fref, mut fsi, mut ftype) = (None, None, None, None);
                let mut attributes = XmlAttrScanner::from_tag(&formula.opening);
                while let Some((name, value)) = attributes.next()? {
                    let slot = match name {
                        "aca" => &mut aca,
                        "ref" => &mut fref,
                        "si" => &mut fsi,
                        "t" => &mut ftype,
                        _ => bail_fmt!("formula 알 수 없는 {name} 속성"),
                    };
                    ensure_fmt!(slot.replace(value).is_none(), "formula {name} 속성 중복");
                }
                let shared = ftype.as_deref() == Some("shared");
                ensure_fmt!(
                    shared
                        || (ftype.is_none()
                            && fref.is_none()
                            && fsi.is_none()
                            && aca.as_deref().is_none_or(|val| val == "false")),
                    "formula 미지원 속성: row={row_num}, col={col}"
                );
                ensure_fmt!(has_cache, "formula cache 없음: row={row_num}, col={col}");
                if shared {
                    ensure(aca.is_none(), "shared formula 미지원 속성")?;
                    let si = parse_u32(
                        fsi.as_deref().ok_or("shared formula si 없음")?,
                        "shared formula si",
                    )?;
                    let range = fref
                        .as_deref()
                        .map(|text| -> Result<((u32, u32), (u32, u32))> {
                            let (lhs, rhs) = text.split_once(':').unwrap_or((text, text));
                            let unlocked = |src| {
                                parse_ref(src)
                                    .filter(|&(_, _, free)| free)
                                    .map(|(rc, rr, _)| (rc, rr))
                                    .ok_or("shared formula ref 형식 오류")
                            };
                            Ok((unlocked(lhs)?, unlocked(rhs)?))
                        })
                        .transpose()?;
                    let index = si as usize;
                    if decoded.is_empty() {
                        ensure(range.is_none(), "shared formula follower에 ref 범위 있음")?;
                        let hd = self
                            .dense_formula_heads
                            .get_mut(index)
                            .or_else(|| self.sparse_formula_heads.get_mut(&si))
                            .ok_or_else(|| fmt_err!("anchor 없음: si={si}"))?;
                        ensure_fmt!(
                            col == hd.anchor_col
                                && (hd.anchor_row..=hd.last_row).contains(&row_num),
                            "follower 범위 초과: si={si}"
                        );
                        hd.seen = hd.seen.strict_add(1);
                    } else {
                        let (start, end) = range.ok_or("shared formula anchor ref 없음")?;
                        ensure_fmt!(
                            start == (col, row_num) && start.0 == end.0 && start.1 <= end.1,
                            "shared formula ref 범위 오류"
                        );
                        ensure_fmt!(
                            index >= self.dense_formula_heads.len()
                                && !self.sparse_formula_heads.contains_key(&si),
                            "shared formula si 중복: {si}"
                        );
                        let head = SharedFormulaHead {
                            anchor_col: col,
                            anchor_row: row_num,
                            last_row: end.1,
                            seen: 1,
                        };
                        if index == self.dense_formula_heads.len()
                            && self.sparse_formula_heads.is_empty()
                        {
                            push_vec(
                                &mut self.dense_formula_heads,
                                head,
                                1,
                                "shared formula anchor",
                            )?;
                        } else {
                            reserve(
                                self.sparse_formula_heads.try_reserve(1),
                                "shared formula anchor",
                            )?;
                            self.sparse_formula_heads.insert(si, head);
                        }
                    }
                } else {
                    let esc = xml_escape(&decoded, false, "formula escape")?;
                    if formula.opening.raw != "<f>" || esc.as_ref() != formula.body {
                        cell.inner_xml =
                            replace_f_tag(inner, formula.span, "", &esc, "formula XML")?;
                    }
                }
            }
        }
        row.cells.push(cell);
        self.cell_count = self.cell_count.strict_add(1);
        Ok(())
    }
    fn scan_rows(&mut self, sc: &mut XmlScanner<'_>, sd_name: &str) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        let last_col = match self.sheet {
            Sheet::ChangeLog => CL_LAST_COL,
            Sheet::Master => MS_LAST_COL,
        };
        let (mut lc_buf, mut st_buf) = (NumBuffer::new(), NumBuffer::new());
        let lc_text = last_col.format_into(&mut lc_buf);
        let mut seen: Vec<&str> = Vec::new();
        while let Some(info) = sc.next_direct_opening_named_until("row", sd_name, "sheetData")? {
            seen.clear();
            let (mut r_tx, mut s_tx, mut in_ht) = (None, None, None);
            let (mut c_fmt, mut in_c_ht) = (false, false);
            let mut a_sc = XmlAttrScanner::from_tag(&info);
            while let Some((nm, val)) = a_sc.next()? {
                ensure(seen.len() < 128 && !seen.contains(&nm), "XML row 속성 오류")?;
                push_vec(&mut seen, nm, 1, "XML 속성")?;
                ensure_fmt!(
                    nm != "xmlns" && !nm.starts_with("xmlns:"),
                    "row namespace 재정의 미지원"
                );
                match nm {
                    "r" => r_tx = Some(val),
                    "s" => s_tx = Some(val),
                    "ht" => in_ht = Some(val),
                    "customFormat" => c_fmt = parse_xml_bool(nm, &val)?,
                    "customHeight" => in_c_ht = parse_xml_bool(nm, &val)?,
                    _ => {}
                }
            }
            let row_num = parse_u32(r_tx.as_deref().ok_or("row r 속성 없음")?, "row 번호")?;
            ensure_fmt!(
                (1..=MAX_A1_ROW).contains(&row_num) && row_num == row_u32(rows.len()).strict_add(1),
                "row 번호 오류: {row_num}"
            );
            let style = s_tx
                .as_deref()
                .map(|val| {
                    canon_style(
                        parse_u32(val, "row style")?,
                        self.sheet,
                        row_num,
                        None,
                        self.input_styles,
                    )
                })
                .transpose()?;
            let (ht, c_ht) = if self.sheet == Sheet::Master && matches!(row_num, 12 | 13) {
                (Some("27"), true)
            } else {
                (in_ht.as_deref(), in_c_ht)
            };
            let mut attrs_xml = str_cap(info.raw.len().strict_add(16), "row 속성")?;
            attrs_xml.extend([" spans=\"1:", lc_text, "\""]);
            if let Some(sid) = style {
                attrs_xml.extend([" s=\"", sid.format_into(&mut st_buf), "\""]);
            }
            if c_fmt {
                attrs_xml.push_str(" customFormat=\"1\"");
            }
            if let Some(ht_tx) = ht {
                attrs_xml.push_str(" ht=\"");
                push_xml_esc(&mut attrs_xml, ht_tx, true);
                attrs_xml.push('"');
            }
            if c_ht {
                attrs_xml.push_str(" customHeight=\"1\"");
            }
            if rows.len() == rows.capacity() {
                reserve(rows.try_reserve(64), "worksheet row")?;
            }
            let cells = if info.self_closing {
                Vec::new()
            } else {
                vec_cap(last_col as usize, "worksheet row cell")?
            };
            let mut row = Row { attrs_xml, cells };
            if !info.self_closing {
                self.parse_row(sc, info.name, row_num, &mut row)?;
            }
            rows.push(row);
        }
        Ok(rows)
    }
}
impl Worksheet {
    fn canonical_share_formulas(&mut self, sheet: Sheet) -> Result<()> {
        let layout = match sheet {
            Sheet::ChangeLog => CL_LAYOUT,
            Sheet::Master => MS_LAYOUT,
        };
        let mut next_si = 0_u32;
        let col_mask = layout
            .required_cols
            .iter()
            .copied()
            .chain(layout.optional_zero_col)
            .fold(0_u32, |mask, col| mask | 1_u32.unbounded_shl(col));
        let mut col_state = [None; (MS_LAST_COL as usize).strict_add(1)];
        let (mut follow_xml, mut reference) = (String::new(), String::new());
        let start_idx = layout.data_start_row.strict_sub(1) as usize;
        for (row, r_idx) in
            (layout.data_start_row..=row_u32(self.rows.len())).zip(start_idx..self.rows.len())
        {
            let cell_count = or_abort(self.rows.get(r_idx)).cells.len();
            for c_idx in 0..cell_count {
                let cur = or_abort(self.rows.get(r_idx).and_then(|ro| ro.cells.get(c_idx)));
                let col = cur.col;
                if col_mask & 1_u32.unbounded_shl(col) == 0 {
                    continue;
                }
                let state = or_abort(col_state.get_mut(col as usize));
                let prev = *state;
                if prev.is_some_and(|(last, _)| row <= last) || !has_formula_xml(&cur.inner_xml)? {
                    continue;
                }
                let gap = prev.is_some_and(|(last, was_gap)| was_gap || row > last.strict_add(1));
                let max_last = row
                    .strict_add(if gap { MAX_SF_GAP } else { MAX_SF_FOLLOW })
                    .min(MAX_A1_ROW);
                let mut last_row = row;
                while last_row < max_last
                    && self
                        .cell_at(col, last_row.strict_add(1))
                        .map_or(Ok(false), |cand| has_formula_xml(&cand.inner_xml))?
                {
                    last_row = last_row.strict_add(1);
                }
                *state = Some((last_row, gap));
                if last_row.strict_sub(row).strict_add(1) < MIN_SF_CELLS {
                    continue;
                }
                let mut col_buf = [0_u8; 3];
                let col_name = col_name_into(col, &mut col_buf)?;
                reference.clear();
                append_fmt(
                    &mut reference,
                    format_args!(
                        " t=\"shared\" ref=\"{col_name}{row}:{col_name}{last_row}\" si=\"{next_si}\""
                    ),
                );
                follow_xml.clear();
                append_fmt(
                    &mut follow_xml,
                    format_args!("<f t=\"shared\" si=\"{next_si}\"/>"),
                );
                for s_row in row..=last_row {
                    let cell = Self::get_or_create_cell_mut(&mut self.rows, col, s_row)?;
                    ensure(!cell.inner_xml.is_empty(), "shared formula cell 본문 없음")?;
                    if s_row == row {
                        if cell.inner_xml.starts_with("<f>") {
                            reserve_str(
                                &mut cell.inner_xml,
                                reference.len(),
                                "shared formula root",
                            )?;
                            cell.inner_xml.insert_str(2, &reference);
                        } else {
                            let formula = XmlScanner::new(&cell.inner_xml)
                                .next_element_named("f")?
                                .ok_or("formula 태그 없음")?;
                            let dec = decode_xml_entities(formula.body)?;
                            let esc = xml_escape(&dec, false, "formula")?;
                            cell.inner_xml = replace_f_tag(
                                &cell.inner_xml,
                                formula.span,
                                &reference,
                                &esc,
                                "formula",
                            )?;
                        }
                    } else {
                        let span = if cell.inner_xml.starts_with("<f/>") {
                            (0..4).into()
                        } else {
                            XmlScanner::new(&cell.inner_xml)
                                .next_element_named("f")?
                                .map(|el| el.span)
                                .ok_or("formula 태그 없음")?
                        };
                        let extra = follow_xml
                            .len()
                            .saturating_sub(span.end.strict_sub(span.start));
                        reserve_str(&mut cell.inner_xml, extra, "shared formula follower")?;
                        cell.inner_xml.replace_range(span, &follow_xml);
                    }
                }
                next_si = next_si.strict_add(1);
            }
        }
        Ok(())
    }
    fn cell_at(&self, col: u32, row: u32) -> Option<&Cell> {
        row_index(row).and_then(|idx| self.rows.get(idx))?.cell(col)
    }
    pub(crate) fn clear_cells_in_rows_through_col(
        &mut self,
        start_row: u32,
        last_row: u32,
        max_col: u32,
    ) {
        let (start, end) = (
            row_index(start_row).unwrap_or(0),
            (last_row as usize).min(self.rows.len()),
        );
        for row_obj in self.rows.get_mut(start..end).unwrap_or(&mut []) {
            for cell in row_obj
                .cells
                .iter_mut()
                .take_while(|cell| cell.col <= max_col)
            {
                cell.value_type = CellType::Gen;
                cell.inner_xml.clear();
            }
        }
    }
    pub(crate) fn copy_row_style(
        &mut self,
        source_row: u32,
        target_row: u32,
        max_col: u32,
    ) -> Result<()> {
        let copied = row_index(source_row)
            .and_then(|idx| self.rows.get(idx))
            .ok_or_else(|| fmt_err!("style 원본 row 없음: {source_row}"))?
            .try_copy_style(max_col)?;
        *Self::ensure_row_mut(&mut self.rows, target_row, "style 대상 row")? = copied;
        Ok(())
    }
    fn ensure_row_mut<'rows>(
        rows: &'rows mut Vec<Row>,
        row: u32,
        context: &'static str,
    ) -> Result<&'rows mut Row> {
        let idx = or_abort(row_index(row));
        let need = idx.strict_add(1);
        if rows.len() < need {
            reserve(rows.try_reserve(need.strict_sub(rows.len())), context)?;
            rows.resize_with(need, Row::default);
        }
        Ok(or_abort(rows.get_mut(idx)))
    }
    pub(crate) fn get_i32_at(
        &self,
        col: u32,
        row: u32,
        sst: &SharedStringTable,
    ) -> Result<Option<i32>> {
        let text = self.try_get_display_at(col, row, sst)?;
        let trimmed = text.trim();
        Ok((!trimmed.is_empty())
            .then(|| parse_decimal_scaled(trimmed, 0, false, true))
            .flatten()
            .and_then(|val| i32::try_from(val).ok()))
    }
    fn get_or_create_cell_mut(rows: &mut Vec<Row>, col: u32, row: u32) -> Result<&mut Cell> {
        let row_obj = Self::ensure_row_mut(rows, row, "worksheet cell row")?;
        match row_obj.cell_slot(col) {
            Ok(idx) => Ok(or_abort(row_obj.cells.get_mut(idx))),
            Err(idx) => {
                if row_obj.cells.len() == row_obj.cells.capacity() {
                    reserve(row_obj.cells.try_reserve(16), "worksheet cell")?;
                }
                Ok(row_obj.cells.insert_mut(
                    idx,
                    Cell {
                        col,
                        inner_xml: String::new(),
                        style: Some(0),
                        value_type: CellType::Gen,
                    },
                ))
            }
        }
    }
    pub(crate) fn has_any_row_format(&self, row: u32, max_col: u32) -> bool {
        row_index(row)
            .and_then(|idx| self.rows.get(idx))
            .is_some_and(|ro| {
                !ro.attrs_xml.is_empty()
                    || (max_col > 0 && ro.cells.first().is_some_and(|cell| cell.col <= max_col))
            })
    }
    pub(crate) fn has_formula_at(&self, col: u32, row: u32) -> Result<bool> {
        self.cell_at(col, row)
            .map_or(Ok(false), |cell| has_formula_xml(&cell.inner_xml))
    }
    pub(crate) fn replace_rows(&mut self, rows: Vec<Row>) {
        self.rows = rows;
    }
    pub(crate) const fn row_count(&self) -> usize {
        self.rows.len()
    }
    pub(crate) fn row_numbers_from(&self, start: u32) -> RangeInclusive<u32> {
        RangeInclusive {
            start,
            last: row_u32(self.rows.len()),
        }
    }
    fn semantic_validation(&self, sheet: Sheet, sst: &SharedStringTable) -> Result<()> {
        let (name, layout, last_col) = match sheet {
            Sheet::ChangeLog => (CHANGE_LOG_SHEET_NAME, CL_LAYOUT, CL_LAST_COL),
            Sheet::Master => (MASTER_SHEET_NAME, MS_LAYOUT, MS_LAST_COL),
        };
        let req_mask = layout
            .required_cols
            .iter()
            .fold(0_u32, |mask, &col| mask | 1_u32.unbounded_shl(col));
        let (mut last_data, mut last_addr, mut first_issue) = (None, None, None);
        let mut fixed_count = 0_usize;
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            let (mut req_idx, mut opt_formula) = (0_usize, false);
            for cell in &row.cells {
                let col = cell.col;
                let is_addr = sheet == Sheet::Master && col == 6;
                if matches!(cell.value_type, CellType::Sst(_)) || is_addr {
                    let display = Self::try_get_display_for_cell(cell, sst)?;
                    if is_addr && !display.trim().is_empty() {
                        last_addr = Some(row_num);
                    }
                }
                if cell.inner_xml.is_empty() {
                    continue;
                }
                if let Some(raw_val) = simple_v(&cell.inner_xml) {
                    if row_num >= layout.data_start_row
                        && col <= last_col
                        && !decode_xml_entities(raw_val)?.trim().is_empty()
                    {
                        last_data = Some(row_num);
                    }
                    continue;
                }
                let is_follow = row_num >= layout.data_start_row
                    && cell.inner_xml.starts_with("<f t=\"shared\"")
                    && cell.inner_xml.find('>').is_some_and(|gt| {
                        gt > 0 && cell.inner_xml.as_bytes().get(gt.strict_sub(1)) == Some(&b'/')
                    });
                let content = (!is_follow)
                    .then(|| XmlScanner::new(&cell.inner_xml).next_direct_element("worksheet cell"))
                    .transpose()?
                    .flatten();
                let formula_tx = if is_follow {
                    Some("")
                } else {
                    content
                        .as_ref()
                        .filter(|el| el.opening.name == "f")
                        .map(|el| el.body)
                };
                if let Some(raw_f) = formula_tx {
                    let formula = decode_xml_entities(raw_f)?;
                    ensure_fmt!(
                        !formula.contains("#REF!"),
                        "#REF! 수식: {name}!row={row_num}, col={col}"
                    );
                    let fixed = (row_num < layout.data_start_row)
                        .then(|| {
                            layout
                                .fixed_formulas
                                .iter()
                                .find(|&&(fc, fr, _)| (col, row_num) == (fc, fr))
                        })
                        .flatten();
                    if let Some(&(_, _, exp)) = fixed {
                        ensure_fmt!(formula.as_ref() == exp, "{name} 고정 formula 불일치");
                        fixed_count = fixed_count.strict_add(1);
                    }
                    let data = row_num >= layout.data_start_row
                        && ((col <= last_col && req_mask & 1_u32.strict_shl(col) != 0)
                            || layout.optional_zero_col == Some(col));
                    ensure_fmt!(
                        fixed.is_some() || data,
                        "{name} 고정 위치 밖 formula: row={row_num}, col={col}"
                    );
                    if data {
                        if layout.required_cols.get(req_idx) == Some(&col) {
                            req_idx = req_idx.strict_add(1);
                        }
                        if layout.optional_zero_col == Some(col) {
                            opt_formula = true;
                        }
                    }
                }
                let has_payload = formula_tx.is_some()
                    || content
                        .map(|el| {
                            Ok::<_, AppError>(!decode_xml_entities(el.body)?.trim().is_empty())
                        })
                        .transpose()?
                        .unwrap_or(false);
                if row_num >= layout.data_start_row && col <= last_col && has_payload {
                    last_data = Some(row_num);
                }
            }
            if row_num >= layout.data_start_row && first_issue.is_none() {
                if let Some(&col) = layout.required_cols.get(req_idx) {
                    first_issue = Some((row_num, col, "필수 formula가 없습니다"));
                }
                if first_issue.is_none()
                    && let Some(col) = layout.optional_zero_col
                    && !opt_formula
                    && MasterSheetUpdater::get_f64_at(self, col, row_num, sst)?
                        .is_none_or(|val| val != 0_i64)
                {
                    first_issue = Some((row_num, col, "선택 formula 위치는 수동 0이어야 합니다"));
                }
            }
        }
        ensure_fmt!(
            fixed_count == layout.fixed_formulas.len(),
            "{name} 고정 formula 수 불일치"
        );
        if let (Some(data_end), Some((row, col, detail))) = (last_data, first_issue)
            && row <= data_end
        {
            bail_fmt!("{name} 시트의 {detail}: row={row}, col={col}");
        }
        if sheet == Sheet::Master {
            let addr_last = last_addr.unwrap_or(MS_LAYOUT.data_start_row);
            ensure_fmt!(
                last_data == Some(addr_last),
                "유류비 데이터/주소 마지막 행 불일치"
            );
        }
        Ok(())
    }
    pub(crate) fn set_existing_cell_style_in_range(
        &mut self,
        row: u32,
        start_col: u32,
        end_col: u32,
        style: u32,
    ) -> Result<()> {
        let row_obj = row_index(row)
            .and_then(|idx| self.rows.get_mut(idx))
            .ok_or_else(|| fmt_err!("style 대상 row 없음: {row}"))?;
        let start = row_obj.cells.partition_point(|cell| cell.col < start_col);
        let count = end_col
            .checked_sub(start_col)
            .and_then(|val| val.checked_add(1))
            .ok_or("column 범위 오류")? as usize;
        let end = start.checked_add(count).ok_or("cell 범위 오류")?;
        let cells = row_obj.cells.get_mut(start..end).ok_or("cell 범위 오류")?;
        for (exp_col, cell) in (start_col..=end_col).zip(cells) {
            ensure_fmt!(
                cell.col == exp_col,
                "style 대상 cell 없음: row={row}, col={exp_col}"
            );
            cell.style = Some(style);
        }
        Ok(())
    }
    pub(crate) fn set_formula_at_with_cache(
        &mut self,
        col: u32,
        row: u32,
        formula: Option<&str>,
        value: Option<&str>,
        string_value: bool,
    ) -> Result<()> {
        let f_len = formula
            .map(|raw| xml_escaped_len(raw, false, "formula"))
            .transpose()?;
        let v_len = value
            .map(|raw| xml_escaped_len(raw, false, "cache"))
            .transpose()?;
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        cell.value_type = if string_value || value.is_none() {
            CellType::Str
        } else {
            CellType::Gen
        };
        let cached = value.unwrap_or("");
        let inner = &mut cell.inner_xml;
        inner.clear();
        let cap = f_len
            .map_or(34_usize, |len| 14_usize.strict_add(len))
            .strict_add(v_len.unwrap_or(0));
        reserve(inner.try_reserve_exact(cap), "formula/cache XML")?;
        let append_tag =
            |output: &mut String, tag: &str, text: Option<&str>, esc_len: Option<usize>| {
                if let Some(raw) = text.filter(|raw| !raw.is_empty()) {
                    output.extend(["<", tag, ">"]);
                    if esc_len == Some(raw.len()) {
                        output.push_str(raw);
                    } else {
                        push_xml_esc(output, raw, false);
                    }
                    output.extend(["</", tag, ">"]);
                } else if text.is_some() {
                    output.extend(["<", tag, "></", tag, ">"]);
                } else {
                    output.extend(["<", tag, "/>"]);
                }
            };
        append_tag(inner, "f", formula, f_len);
        append_tag(inner, "v", (!cached.is_empty()).then_some(cached), v_len);
        Ok(())
    }
    fn set_formula_cached_value_at(
        &mut self,
        col: u32,
        row: u32,
        value: Option<&str>,
    ) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        cell.value_type = if value.is_none() {
            CellType::Str
        } else {
            CellType::Gen
        };
        ensure_fmt!(
            !cell.inner_xml.is_empty(),
            "수식 cache 대상 cell 비어 있음: row={row}, col={col}"
        );
        let encoded = value
            .map(|raw| xml_escape(raw, false, "cache"))
            .transpose()?;
        let val_tx = encoded.as_deref().unwrap_or("");
        let xml = &mut cell.inner_xml;
        let el = XmlScanner::new(xml)
            .next_element_named("v")?
            .ok_or("v 태그 없음")?;
        if val_tx.is_empty() {
            if !el.opening.self_closing {
                xml.replace_range(el.opening.end..el.span.end, "/>");
            }
        } else if el.opening.self_closing {
            let prefix = or_abort(el.opening.raw.strip_suffix("/>"));
            let mut rep = str_cap(
                prefix.len().strict_add(5).strict_add(val_tx.len()),
                "XML 치환",
            )?;
            rep.extend([prefix, ">", val_tx, "</v>"]);
            xml.replace_range(el.span, &rep);
        } else {
            xml.replace_range(el.body_span, val_tx);
        }
        Ok(())
    }
    pub(crate) fn set_i32_at(&mut self, col: u32, row: u32, value: Option<i32>) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        cell.value_type = CellType::Gen;
        cell.inner_xml.clear();
        if let Some(num) = value {
            cell.inner_xml
                .extend(["<v>", num.format_into(&mut NumBuffer::new()), "</v>"]);
        }
        Ok(())
    }
    pub(crate) fn take_rows(&mut self) -> Vec<Row> {
        mem::take(&mut self.rows)
    }
    fn to_xml(&self, sheet: Sheet, last_data_row: u32) -> Result<(String, usize)> {
        let (prefix_src, suffix_src, dim_ref, end_col) = match sheet {
            Sheet::ChangeLog => (CL_PREFIX, CL_SUFFIX, "A1:M243", "M"),
            Sheet::Master => (MS_PREFIX, MS_SUFFIX, "A1:W884", "W"),
        };
        let (mut out, mut suffix) = (
            canonical_excel_fragment(prefix_src)?,
            canonical_excel_fragment(suffix_src)?,
        );
        let max_row = self
            .rows
            .iter()
            .rposition(|row| !row.cells.is_empty())
            .map_or(1, |idx| row_u32(idx.strict_add(1)));
        let (mut r_buf, mut n_buf) = (String::with_capacity(24), NumBuffer::new());
        r_buf.extend(["A1:", end_col, max_row.format_into(&mut n_buf)]);
        replace_canon(&mut out, dim_ref, &r_buf, "dimension")?;
        match sheet {
            Sheet::Master => {
                r_buf.clear();
                r_buf.extend(["A14:W", last_data_row.max(14).format_into(&mut n_buf)]);
                replace_canon(&mut suffix, "A14:W882", &r_buf, "autoFilter")?;
                let last_tx = row_u32(self.rows.len()).format_into(&mut n_buf);
                for (old, pre, ctx) in [
                    ("A15:W884", "A15:W", "sortState"),
                    ("A15:A884", "A15:A", "sortCondition"),
                ] {
                    r_buf.clear();
                    r_buf.extend([pre, last_tx]);
                    replace_canon(&mut suffix, old, &r_buf, ctx)?;
                }
            }
            Sheet::ChangeLog => {
                let start = CL_LAYOUT.data_start_row;
                let mut s_buf = NumBuffer::new();
                let start_tx = start.format_into(&mut s_buf);
                let end_tx = (last_data_row > start).then(|| last_data_row.format_into(&mut n_buf));
                for (col, old) in [("G", "G4:G52"), ("J", "J4:J52"), ("M", "M4:M52")] {
                    r_buf.clear();
                    r_buf.extend([col, start_tx]);
                    if let Some(end_row) = end_tx {
                        r_buf.extend([":", col, end_row]);
                    }
                    replace_canon(&mut suffix, old, &r_buf, "conditionalFormatting")?;
                }
            }
        }
        let extra = self.rows.iter().fold(suffix.len(), |cap, row| {
            row.cells.iter().fold(
                cap.strict_add(row.attrs_xml.len()).strict_add(64),
                |cc, cell| cc.strict_add(cell.inner_xml.len()).strict_add(80),
            )
        });
        reserve_str(&mut out, extra, "worksheet XML")?;
        let (mut sst_refs, mut row_buf, mut st_buf, mut idx_buf, mut col_buf) = (
            0_usize,
            NumBuffer::new(),
            NumBuffer::new(),
            NumBuffer::new(),
            [0_u8; 3],
        );
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            let row_tx = row_num.format_into(&mut row_buf);
            out.extend(["<row r=\"", row_tx, "\"", &row.attrs_xml]);
            if row.cells.is_empty() {
                out.push_str("/>");
                continue;
            }
            out.push('>');
            for cell in &row.cells {
                out.extend([
                    "<c r=\"",
                    col_name_into(cell.col, &mut col_buf)?,
                    row_tx,
                    "\"",
                ]);
                if let Some(style) = cell.style {
                    out.extend([" s=\"", style.format_into(&mut st_buf), "\""]);
                }
                match cell.value_type {
                    CellType::Sst(idx) => {
                        sst_refs = sst_refs.strict_add(1);
                        out.extend([" t=\"s\"><v>", idx.format_into(&mut idx_buf), "</v></c>"]);
                    }
                    CellType::Str if !cell.inner_xml.is_empty() => {
                        out.extend([" t=\"str\">", &cell.inner_xml, "</c>"]);
                    }
                    CellType::Str => out.push_str(" t=\"str\"/>"),
                    CellType::Gen if !cell.inner_xml.is_empty() => {
                        out.extend([">", &cell.inner_xml, "</c>"]);
                    }
                    CellType::Gen => out.push_str("/>"),
                }
            }
            out.push_str("</row>");
        }
        out.push_str(&suffix);
        Ok((out, sst_refs))
    }
    pub(crate) fn truncate_rows_after(&mut self, last_row_to_keep: u32) {
        self.rows.truncate(last_row_to_keep as usize);
    }
    pub(crate) fn try_get_display_at<'text>(
        &'text self,
        col: u32,
        row: u32,
        sst: &'text SharedStringTable,
    ) -> Result<Cow<'text, str>> {
        self.cell_at(col, row).map_or_else(
            || Ok(Cow::Borrowed("")),
            |cell| Self::try_get_display_for_cell(cell, sst),
        )
    }
    fn try_get_display_for_cell<'text>(
        cell: &'text Cell,
        sst: &'text SharedStringTable,
    ) -> Result<Cow<'text, str>> {
        if let CellType::Sst(idx) = cell.value_type {
            return sst
                .get(idx)
                .map(Cow::Borrowed)
                .ok_or_else(|| fmt_err!("SST index 범위 오류: {idx}"));
        }
        let raw_v = match simple_v(&cell.inner_xml) {
            Some(body) => body,
            None => extract_first_tag_text(&cell.inner_xml, "v")?.unwrap_or(""),
        };
        decode_xml_entities(raw_v)
    }
    pub(crate) fn try_get_fixed_text_at<'strings>(
        &self,
        col: u32,
        row: u32,
        sst: &'strings SharedStringTable,
    ) -> Result<&'strings str> {
        let Some(cell) = self.cell_at(col, row) else {
            return Ok("");
        };
        if let CellType::Sst(idx) = cell.value_type {
            return sst
                .get(idx)
                .ok_or_else(|| fmt_err!("SST index 범위 오류: {idx}"));
        }
        ensure_fmt!(
            cell.inner_xml.is_empty(),
            "텍스트 cell이 shared string이 아님: row={row}, col={col}"
        );
        Ok("")
    }
    fn validate_fixed_header(&self, sheet: Sheet, sst: &SharedStringTable) -> Result<()> {
        let (name, h_row, headers, last_col) = match sheet {
            Sheet::Master => (MASTER_SHEET_NAME, 14, MS_HEADERS, MS_LAST_COL),
            Sheet::ChangeLog => (CHANGE_LOG_SHEET_NAME, 3, CL_HEADERS, CL_LAST_COL),
        };
        let actual_last = self
            .rows
            .iter()
            .filter_map(|row| row.cells.last().map(|cell| cell.col))
            .max()
            .unwrap_or(1);
        ensure_fmt!(actual_last == last_col, "{name} 마지막 열 불일치");
        let h_cells: &[Cell] = row_index(h_row)
            .and_then(|idx| self.rows.get(idx))
            .map_or(&[], |row| row.cells.as_slice());
        let mut iter = h_cells.iter().peekable();
        for (col, exp) in (1_u32..).zip(headers.split('|')) {
            let actual = iter.next_if(|cell| cell.col == col).map_or_else(
                || Ok(Cow::Borrowed("")),
                |cell| Self::try_get_display_for_cell(cell, sst),
            )?;
            ensure_fmt!(actual.as_ref() == exp, "{name} 헤더 불일치: col={col}");
        }
        Ok(())
    }
}
impl Row {
    fn cell(&self, col: u32) -> Option<&Cell> {
        self.cells.get(self.cell_slot(col).ok()?)
    }
    fn cell_slot(&self, col: u32) -> CoreResult<usize, usize> {
        if let Some(idx) = col
            .checked_sub(1)
            .and_then(|val| usize::try_from(val).ok())
            .filter(|&idx| self.cells.get(idx).is_some_and(|cell| cell.col == col))
        {
            return Ok(idx);
        }
        self.cells.binary_search_by_key(&col, |cell| cell.col)
    }
    pub(crate) fn try_copy_style(&self, max_col: u32) -> Result<Self> {
        let count = self.cells.partition_point(|cell| cell.col <= max_col);
        let mut cells = vec_cap(count, "row style 복사")?;
        cells.extend(self.cells.iter().take(count).map(|cell| Cell {
            col: cell.col,
            inner_xml: String::new(),
            style: cell.style,
            value_type: CellType::Gen,
        }));
        Ok(Self {
            attrs_xml: copy_text(&self.attrs_xml)?,
            cells,
        })
    }
}
fn canon_style(
    style: u32,
    sheet: Sheet,
    row: u32,
    col: Option<u32>,
    map: &CanonicalStyleMap,
) -> Result<u32> {
    if sheet == Sheet::Master
        && let Some(column) = col
    {
        match (row, column) {
            (1, 1..=7) => return Ok(24),
            (2, 1) => return Ok(25),
            (3, 1..=2) => return Ok(22),
            _ => {}
        }
    }
    map.get(style as usize)
        .copied()
        .flatten()
        .ok_or_else(|| fmt_err!("style 정규형 변환 불가: {style}"))
}
fn col_name_into(mut col: u32, buf: &mut [u8; 3]) -> Result<&str> {
    (1..=0x4000_u32)
        .contains(&col)
        .ok_or_else(|| fmt_err!("Excel column 범위 초과: {col}"))?;
    let s_idx = col.strict_sub(1) as usize;
    if let Some(single) = SINGLE_COLS.get(s_idx..s_idx.strict_add(1)) {
        return Ok(single);
    }
    let mut idx = buf.len();
    while col > 0 {
        let base = col.strict_sub(1);
        idx = idx.strict_sub(1);
        *or_abort(buf.get_mut(idx)) = b'A'.strict_add(base.rem_euclid(26).to_le_bytes()[0]);
        col = base.div_euclid(26);
    }
    Ok(or_abort(str::from_utf8(or_abort(buf.get(idx..))).ok()))
}
pub(crate) fn format_excel_ratio_into(out: &mut String, numerator: i128, denominator: i128) {
    const POW5_16: u64 = 152_587_890_625;
    const SCALE: u64 = 1_000_000_000;
    let neg = numerator != 0 && numerator.is_negative() != denominator.is_negative();
    let (den_abs, num_abs) = (denominator.unsigned_abs(), numerator.unsigned_abs());
    let (whole, mut rem) = (num_abs.div_euclid(den_abs), num_abs.rem_euclid(den_abs));
    if rem == 0 {
        out.clear();
        if neg {
            out.push('-');
        }
        push_dec!(out, whole);
        return;
    }
    let fast = u64::try_from(num_abs)
        .ok()
        .zip(u64::try_from(den_abs).ok())
        .filter(|&(num, den)| num >= den && num.is_multiple_of(SCALE) && den.is_multiple_of(SCALE))
        .and_then(|(num, den)| {
            let (sn, sd) = (num.div_euclid(SCALE), den.div_euclid(SCALE));
            (!u128::from(sn.rem_euclid(sd))
                .strict_mul(u128::from(POW5_16))
                .is_multiple_of(u128::from(sd >> sd.trailing_zeros())))
            .then(|| u32::try_from(sn).ok().zip(u32::try_from(sd).ok()))
            .flatten()
        })
        .map(|(num, den)| {
            f64::from_bits((f64::from(num) / f64::from(den)).to_bits() ^ (u64::from(neg) << 63_u32))
        });
    let value = fast.unwrap_or_else(|| {
        out.clear();
        if neg {
            out.push('-');
        }
        push_dec!(out, whole);
        out.push('.');
        for _ in 0_u32..32_u32 {
            rem = rem.strict_mul(10);
            let [digit, ..] = rem.div_euclid(den_abs).to_le_bytes();
            out.push(char::from(b'0'.strict_add(digit)));
            rem = rem.rem_euclid(den_abs);
            if rem == 0 {
                break;
            }
        }
        if rem == 0 {
            0.0_f64
        } else {
            out.parse::<f64>().unwrap_or_else(|_| process::abort())
        }
    });
    if rem == 0 {
        return;
    }
    out.clear();
    append_fmt(out, format_args!("{value:.16e}"));
    let exp_pos = or_abort(out.rfind('e'));
    let exp = or_abort(
        or_abort(out.get(exp_pos.strict_add(1)..))
            .parse::<i32>()
            .ok(),
    );
    let sign_len = usize::from(out.starts_with('-'));
    let pt = sign_len.strict_add(or_abort(or_abort(out.get(sign_len..exp_pos)).find('.')));
    let dec_pos = exp.strict_add(1);
    out.truncate(exp_pos);
    out.remove(pt);
    let digits = out.len().strict_sub(sign_len);
    if dec_pos <= 0_i32 {
        out.insert_str(sign_len, "0.");
        let z_pos = sign_len.strict_add(2);
        for _ in 0..dec_pos.unsigned_abs() {
            out.insert(z_pos, '0');
        }
    } else {
        let pos = or_abort(usize::try_from(dec_pos).ok());
        if let Some(pad) = pos.checked_sub(digits) {
            out.extend(repeat_n('0', pad));
        } else {
            out.insert(sign_len.strict_add(pos), '.');
        }
    }
    if out.contains('.') {
        out.truncate(out.trim_end_matches('0').trim_suffix('.').len());
    }
}
fn has_edge_ws(text: &str) -> bool {
    text.chars().next().is_some_and(char::is_whitespace)
        || text.chars().next_back().is_some_and(char::is_whitespace)
}
fn has_formula_xml(inner: &str) -> Result<bool> {
    if inner.starts_with("<f/>") || inner.starts_with("<f>") {
        return Ok(true);
    }
    if simple_v(inner).is_some() {
        return Ok(false);
    }
    Ok(extract_first_tag_text(inner, "f")?.is_some())
}
pub(crate) const fn is_shared_formula_follower(offset: usize, total: usize) -> bool {
    let group = (MAX_SF_FOLLOW as usize).strict_add(1);
    let off = offset.rem_euclid(group);
    off != 0 && total.strict_sub(offset.strict_sub(off)) >= MIN_SF_CELLS as usize
}
pub(crate) fn parse_decimal_scaled(
    text: &str,
    scale_exp: i64,
    strict_commas: bool,
    round_half_away: bool,
) -> Option<i64> {
    let (neg, digits) = text
        .strip_prefix('-')
        .map_or_else(|| (false, text.trim_prefix('+')), |rest| (true, rest));
    let (mantissa, exp) = match digits.split_once(['e', 'E']) {
        Some((mant, _)) if !strict_commas && mant.contains(',') => return None,
        Some((mant, exp_tx)) => (mant, exp_tx.parse::<i64>().ok()?),
        None => (digits, 0_i64),
    };
    let int_d = mantissa
        .bytes()
        .take_while(|&byte| byte != b'.')
        .filter(u8::is_ascii_digit)
        .count();
    let dec_pos = i64::try_from(int_d)
        .ok()?
        .checked_add(exp)?
        .checked_add(scale_exp)?;
    let (mut whole, mut pos, mut grp) = (0_i64, 0_i64, 0_u32);
    let (mut has_grp, mut round_up, mut saw_digit, mut seen_dec) = (false, false, false, false);
    for byte in mantissa.bytes() {
        match byte {
            b',' if !seen_dec && strict_commas => {
                if grp == 0 || (has_grp && grp != 3) || grp > 3 {
                    return None;
                }
                (has_grp, grp) = (true, 0);
            }
            b',' if !seen_dec => {}
            b'.' if !seen_dec && (!strict_commas || !has_grp || grp == 3) => seen_dec = true,
            b'0'..=b'9' => {
                saw_digit = true;
                grp = grp.strict_add(u32::from(!seen_dec));
                let digit = i64::from(byte.strict_sub(b'0'));
                match pos.cmp(&dec_pos) {
                    Ordering::Less => whole = whole.checked_mul(10)?.checked_add(digit)?,
                    Ordering::Equal if round_half_away => round_up = digit >= 5_i64,
                    Ordering::Equal | Ordering::Greater if !round_half_away && digit != 0 => {
                        return None;
                    }
                    Ordering::Equal | Ordering::Greater => {}
                }
                pos = pos.strict_add(1);
            }
            _ => return None,
        }
    }
    if !saw_digit || (strict_commas && has_grp && grp != 3) {
        return None;
    }
    if whole != 0 && dec_pos > pos {
        whole =
            whole.checked_mul(10_i64.checked_pow(u32::try_from(dec_pos.strict_sub(pos)).ok()?)?)?;
    }
    let mag = whole.checked_add(i64::from(round_up))?;
    if neg { mag.checked_neg() } else { Some(mag) }
}
fn parse_ref(reference: &str) -> Option<(u32, u32, bool)> {
    let bytes = reference.as_bytes();
    let mut idx = usize::from(bytes.first() == Some(&b'$'));
    let col_locked = idx != 0;
    let (start, mut col) = (idx, 0_u32);
    while let Some(&ch) = bytes.get(idx)
        && ch.is_ascii_alphabetic()
    {
        if idx.strict_sub(start) >= 3 {
            return None;
        }
        let letter = u32::from(ch.to_ascii_uppercase())
            .checked_sub(u32::from('A'))?
            .checked_add(1)?;
        col = col.checked_mul(26)?.checked_add(letter)?;
        idx = idx.strict_add(1);
    }
    if idx == start || !(1..=0x4000_u32).contains(&col) {
        return None;
    }
    let row_locked = bytes.get(idx) == Some(&b'$');
    idx = idx.strict_add(usize::from(row_locked));
    let row = bytes.get(idx..)?.iter().try_fold(0_u32, |row, &ch| {
        ch.is_ascii_digit()
            .then(|| {
                row.strict_mul(10)
                    .strict_add(u32::from(ch.strict_sub(b'0')))
            })
            .filter(|&val| val <= MAX_A1_ROW)
    })?;
    (row != 0).then_some((col, row, !col_locked && !row_locked))
}
fn parse_u32(value: &str, field: &str) -> Result<u32> {
    parse_unsigned_u32(
        value,
        format_args!("{field}이(가) 음이 아닌 10진수가 아닙니다."),
        format_args!("{field} 해석 실패"),
    )
}
fn parse_xml_bool(name: &str, value: &str) -> Result<bool> {
    match value {
        "0" | "false" => Ok(false),
        "1" | "true" => Ok(true),
        _ => Err(fmt_err!("worksheet {name} boolean 오류: {value}")),
    }
}
fn push_xml_esc(out: &mut String, text: &str, is_attr: bool) {
    let mut start = 0_usize;
    for (idx, &byte) in text.as_bytes().iter().enumerate() {
        if let Some(rep) = xml_esc_rep(byte, is_attr) {
            out.push_str(or_abort(text.get(start..idx)));
            out.push_str(rep);
            start = idx.strict_add(1);
        }
    }
    out.push_str(or_abort(text.get(start..)));
}
fn replace_canon(xml: &mut String, old: &str, new: &str, context: &str) -> Result<()> {
    let start = xml
        .find(old)
        .ok_or_else(|| fmt_err!("Excel 정규형 {context} 표식 없음"))?;
    let end = start.strict_add(old.len());
    ensure_fmt!(
        !xml.get(end..).is_some_and(|rem| rem.contains(old)),
        "Excel 정규형 {context} 표식 중복"
    );
    xml.replace_range(start..end, new);
    Ok(())
}
fn replace_f_tag(
    inner: &str,
    span: Range<usize>,
    attrs: &str,
    esc_body: &str,
    ctx: &'static str,
) -> Result<String> {
    let (pre, suf) = (
        or_abort(inner.get(..span.start)),
        or_abort(inner.get(span.end..)),
    );
    let len = pre
        .len()
        .strict_add(7)
        .strict_add(attrs.len())
        .strict_add(esc_body.len())
        .strict_add(suf.len());
    let mut updated = str_cap(len, ctx)?;
    updated.extend([pre, "<f", attrs, ">", esc_body, "</f>", suf]);
    Ok(updated)
}
fn reserve_str(out: &mut String, additional: usize, context: &'static str) -> Result<()> {
    reserve(out.try_reserve(additional), context)
}
const fn row_index(row: u32) -> Option<usize> {
    (row as usize).checked_sub(1)
}
fn row_u32(len: usize) -> u32 {
    u32::try_from(len).unwrap_or_else(|_| process::abort())
}
fn simple_v(inner: &str) -> Option<&str> {
    inner
        .strip_prefix("<v>")
        .and_then(|rest| rest.strip_suffix("</v>"))
        .filter(|body| !body.as_bytes().contains(&b'<'))
}
const fn xml_esc_rep(byte: u8, is_attr: bool) -> Option<&'static str> {
    match byte {
        b'\t' if is_attr => Some("&#x9;"),
        b'\n' if is_attr => Some("&#xA;"),
        b'\r' => Some("&#xD;"),
        b'&' => Some("&amp;"),
        b'<' => Some("&lt;"),
        b'>' => Some("&gt;"),
        b'"' if is_attr => Some("&quot;"),
        b'\'' if is_attr => Some("&apos;"),
        _ => None,
    }
}
fn xml_escape<'text>(
    text: &'text str,
    is_attr: bool,
    ctx: &'static str,
) -> Result<Cow<'text, str>> {
    let cap = xml_escaped_len(text, is_attr, ctx)?;
    if cap == text.len() {
        return Ok(Cow::Borrowed(text));
    }
    let mut out = str_cap(cap, "XML escape")?;
    push_xml_esc(&mut out, text, is_attr);
    Ok(Cow::Owned(out))
}
fn xml_escaped_len(text: &str, is_attr: bool, ctx: &str) -> Result<usize> {
    let bytes = text.as_bytes();
    let mut total = bytes.len();
    for (idx, &byte) in bytes.iter().enumerate() {
        match byte {
            0x00..=0x7F => {
                ensure_fmt!(
                    byte >= 0x20 || matches!(byte, b'\t' | b'\n' | b'\r'),
                    "{ctx}: XML 1.0 미허용 U+{byte:04X}"
                );
                if let Some(rep) = xml_esc_rep(byte, is_attr) {
                    total = total.strict_add(rep.len().strict_sub(1));
                }
            }
            0xEF => {
                if let Some(&[0xBF, third @ (0xBE | 0xBF)]) =
                    bytes.get(idx.strict_add(1)..idx.strict_add(3))
                {
                    let code = if third == 0xBE {
                        0xFFFE_u32
                    } else {
                        0xFFFF_u32
                    };
                    bail_fmt!("{ctx}: XML 1.0 미허용 U+{code:04X}");
                }
            }
            _ => {}
        }
    }
    Ok(total)
}
