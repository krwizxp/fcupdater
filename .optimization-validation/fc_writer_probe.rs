pub(crate) fn probe_intern_xml(corpus: &[String]) -> Result<String> {
    let mut table = SharedStringTable { entries: Vec::new(), index: HashMap::new() };
    for text in corpus { table.intern(text)?; }
    table.to_xml(corpus.len())
}
