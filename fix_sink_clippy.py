import re

with open('crates/parquet-sink/src/sink.rs', 'r') as f:
    content = f.read()

content = content.replace('    async fn run(&mut self, mut input: PipelineReceiver) -> Result<(), PipelineError> {', '    #[allow(clippy::collapsible_if)]\n    async fn run(&mut self, mut input: PipelineReceiver) -> Result<(), PipelineError> {')

with open('crates/parquet-sink/src/sink.rs', 'w') as f:
    f.write(content)
