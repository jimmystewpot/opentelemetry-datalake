import re

with open("crates/parquet-sink/src/uploader.rs", "r") as f:
    content = f.read()

# Add UploaderMessage enum before UploaderSender
content = re.sub(
    r"pub struct UploaderSender",
    "pub enum UploaderMessage {\n    Chunk(bytes::Bytes),\n    Finish,\n}\n\n#[derive(Debug)]\npub struct UploaderSender",
    content
)

# Change tx type
content = re.sub(
    r"tx: tokio::sync::mpsc::Sender<bytes::Bytes>,",
    "tx: tokio::sync::mpsc::Sender<UploaderMessage>,",
    content
)

# In send_chunk:
content = re.sub(
    r"match self\.tx\.try_send\(chunk\) \{",
    "match self.tx.try_send(UploaderMessage::Chunk(chunk)) {",
    content
)

# In send_chunk blocking_send:
content = re.sub(
    r"\.blocking_send\(chunk\)",
    ".blocking_send(UploaderMessage::Chunk(chunk))",
    content
)

# In send_chunk TrySendError::Full(chunk):
content = re.sub(
    r"Err\(tokio::sync::mpsc::error::TrySendError::Full\(chunk\)\) => self\n\s*\.tx\n\s*\.blocking_send\(chunk\)",
    "Err(tokio::sync::mpsc::error::TrySendError::Full(UploaderMessage::Chunk(chunk))) => self\n                .tx\n                .blocking_send(UploaderMessage::Chunk(chunk))",
    content
)

# In send_chunk_async:
content = re.sub(
    r"self\.tx\.send\(chunk\)\.await",
    "self.tx.send(UploaderMessage::Chunk(chunk)).await",
    content
)

# In finish:
content = re.sub(
    r"pub fn finish\(self\) -> Result<\(\), ParquetSinkError> \{\n        drop\(self\.tx\);\n        Ok\(\(\)\)",
    """pub fn finish(self) -> Result<(), ParquetSinkError> {
        // Ignore send errors if receiver is already closed/aborted
        let _ = self.tx.blocking_send(UploaderMessage::Finish);
        drop(self.tx);
        Ok(())""",
    content
)

# In AsyncUploader::start:
content = re.sub(
    r"tokio::sync::mpsc::channel::<bytes::Bytes>\(DEFAULT_CHANNEL_CAPACITY\);",
    "tokio::sync::mpsc::channel::<UploaderMessage>(DEFAULT_CHANNEL_CAPACITY);",
    content
)

# Update rx.recv loop
loop_pattern = r"chunk = rx\.recv\(\) => \{.*?None => \{\s*break;\s*\}\s*\}\s*\}"
new_loop = """msg = rx.recv() => {
                        match msg {
                            Some(UploaderMessage::Chunk(bytes)) => {
                                if let Err(e) = writer.write(bytes).await {
                                    let _ = writer.abort().await;
                                    return Err(ParquetSinkError::from(e));
                                }
                            }
                            Some(UploaderMessage::Finish) => {
                                break;
                            }
                            None => {
                                aborted = true;
                                break;
                            }
                        }
                    }"""

content = re.sub(loop_pattern, new_loop, content, flags=re.DOTALL)

with open("crates/parquet-sink/src/uploader.rs", "w") as f:
    f.write(content)

