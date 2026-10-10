import sys

with open('crates/parquet-sink/src/partition.rs', 'r') as f:
    content = f.read()

# We want to replace the conflict markers.
# Since it's simply two test blocks that git confused because they both end the file,
# we can find the exact text of the conflict block and replace it with both.

start = content.find("<<<<<<< HEAD")
end = content.find(">>>>>>> 5e08d2ecb7963f0ae5dec0b302a916a72cfebb97") + len(">>>>>>> 5e08d2ecb7963f0ae5dec0b302a916a72cfebb97")

if start == -1 or end == -1:
    print("Could not find conflict markers")
    sys.exit(1)

conflict_block = content[start:end]

lines = conflict_block.split("\n")
head_block = []
their_block = []
mode = "none"

for line in lines:
    if line.startswith("<<<<<<< HEAD"):
        mode = "head"
    elif line.startswith("======="):
        mode = "theirs"
    elif line.startswith(">>>>>>>"):
        mode = "none"
    else:
        if mode == "head":
            head_block.append(line)
        elif mode == "theirs":
            their_block.append(line)

resolved = "\n".join(head_block) + "\n    }\n\n    #[tokio::test]\n" + "\n".join(their_block)

new_content = content[:start] + resolved + content[end:]

with open('crates/parquet-sink/src/partition.rs', 'w') as f:
    f.write(new_content)
