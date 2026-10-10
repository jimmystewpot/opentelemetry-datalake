#!/bin/bash
# A simple way to resolve this is to manually accept both tests.
# Since it's a conflict block, I can just remove the markers and keep all the test blocks.
# Let's use sed to strip the markers. But wait, `<<<<<<< HEAD`, `=======`, `>>>>>>> ...`
# In `crates/parquet-sink/src/partition.rs`

sed -i '' -e '/<<<<<<< HEAD/d' -e '/=======/d' -e '/>>>>>>> 5e08d2ecb7963f0ae5dec0b302a916a72cfebb97/d' crates/parquet-sink/src/partition.rs
