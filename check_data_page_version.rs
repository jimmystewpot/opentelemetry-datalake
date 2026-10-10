fn main() {
    let props = parquet::file::properties::WriterProperties::builder().set_data_page_size_limit(1);
    // let's see if set_data_page_version exists
    // props.set_data_page_version
}
