fn api_audit(mode:&str,path:&str,loops:usize)->Result<()> {
 if mode=="capture" {let bytes=source_download::SourceDownload::default().refresh_source()?;std::fs::write(path,bytes)?;return Ok(());}
 if mode=="update" {
  let mut output=Vec::new();OPT_ALLOC_COUNT.store(0,std::sync::atomic::Ordering::Relaxed);OPT_ALLOC_BYTES.store(0,std::sync::atomic::Ordering::Relaxed);let started=std::time::Instant::now();
  UpdateRun{master_path:Path::new(path),out:&mut output,save_verification:SaveVerification::Verify}.run()?;
  let nanos=started.elapsed().as_nanos();let count=OPT_ALLOC_COUNT.load(std::sync::atomic::Ordering::Relaxed);let bytes=OPT_ALLOC_BYTES.load(std::sync::atomic::Ordering::Relaxed);println!("{{\"nanos\":{nanos},\"allocations\":{count},\"allocation_bytes\":{bytes},\"checksum\":1}}");return Ok(());
 }
 let parse=||->Result<()> {let path=Path::new(path);let file=temp_entry::open_regular(path,false)?;let container=excel::xlsx_container::XlsxContainer::from_validated_file(file,path)?;let _book=excel::writer::Workbook::from_container(container)?;Ok(())};
 if loops==0 {return parse();}
 OPT_ALLOC_COUNT.store(0,std::sync::atomic::Ordering::Relaxed);OPT_ALLOC_BYTES.store(0,std::sync::atomic::Ordering::Relaxed); let started=std::time::Instant::now();let mut sum=0_u64;
 for _ in 0..loops {parse()?;sum+=1;}
 let count=OPT_ALLOC_COUNT.load(std::sync::atomic::Ordering::Relaxed);let bytes=OPT_ALLOC_BYTES.load(std::sync::atomic::Ordering::Relaxed);println!("{{\"nanos\":{},\"allocations\":{},\"allocation_bytes\":{},\"checksum\":{}}}",started.elapsed().as_nanos(),count,bytes,sum); Ok(())
}

