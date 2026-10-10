import re

with open('crates/parquet-sink/src/partition.rs', 'r') as f:
    content = f.read()

# Replace the block with the concatenated versions.
# We'll just extract the HEAD block and the cb3004 (or whatever) block, and concatenate them.
pattern = re.compile(r'<<<<<<< HEAD\n(.*?)\n=======\n(.*?)\n>>>>>>> [a-f0-9]+', re.DOTALL)

def replacer(match):
    return match.group(1).strip() + "\n    }\n\n    #[tokio::test]\n" + match.group(2).strip()

content = pattern.sub(replacer, content)

# But wait, the second block also didn't have its closing brace! Let's carefully fix the file instead using simple string replacement or manually writing the end of the file.
