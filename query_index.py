#!/usr/bin/env python3

import sys
import time
import numpy as np

from pathlib import Path

assert len(sys.argv) == 2, f"Usage:\n\n{sys.argv[0]} <data_dir> <tag>\n"

# Input parsing

data_dir = Path(sys.argv[1])
assert data_dir.exists() and data_dir.is_dir()

tags_posts = data_dir / "tags.npy"
index_file = data_dir / "index.npy"

assert tags_posts.exists() and tags_posts.is_file()
assert index_file.exists() and index_file.is_file()

posts = np.load(tags_posts)
index = np.load(index_file)

tags = [470575, 212816, 13197, 29, 1283444]

sets = [set() for _ in tags]

start_time = time.time()
j = 0
for tag in tags:
    start = index[2 * tag]
    end = index[(2 * tag) + 1]

    for i in range(start, end):
        sets[j].add(posts[i])

    j += 1

result = sets[0]
for i in range(1, len(sets)):
    result &= sets[i]
end_time = time.time()

for i in range(len(sets)):
    print(f"{tags[i]}: {len(sets[i])}")
print(sorted(list(result)))
print(f"Took {round(end_time - start_time, 3)} seconds")
