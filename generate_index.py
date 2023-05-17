#!/usr/bin/env python3

import sys
import time
import pickle
import numpy as np

from pathlib import Path
from tqdm import tqdm

assert len(sys.argv) == 2, f"Usage:\n\n{sys.argv[0]} <data_dir>\n"

# Input parsing

data_dir = Path(sys.argv[1])
assert data_dir.exists() and data_dir.is_dir()

tags_pickle = data_dir / "tags.pickle"
posts_pickle = data_dir / "posts.pickle"

assert tags_pickle.exists() and tags_pickle.is_file()
assert posts_pickle.exists() and posts_pickle.is_file()

tags_posts = data_dir / "tags.npy"
index = data_dir / "index.npy"

assert not tags_posts.exists()
assert not index.exists()

start = time.time()
tag_list = None
with tags_pickle.open("rb") as src:
    tag_list = pickle.load(src)
end = time.time()
print(f"Loaded {len(tag_list)} tags in {round(end - start, 3)} seconds")

start = time.time()
post_list = None
with posts_pickle.open("rb") as src:
    post_list = pickle.load(src)
end = time.time()
print(f"Loaded {len(post_list)} posts in {round(end - start, 3)} seconds")

last_tag = 0

posts_size = 0
tag_dict: dict[str, list[int]] = {}
for tag_id, count, name in tag_list:
    last_tag = max(tag_id, last_tag)
    tag_dict[name] = [tag_id, 0, 0]

del tag_list

# Re-count tags, since posts.json and tags.json may be of different ages
for post_id, tags in tqdm(post_list):
    posts_size += len(tags)
    for tag in tags:
        tag_dict[tag][1] += 1

print(f"Total of {posts_size} post IDs to store, tag index of {last_tag + 1} tags")

posts = np.zeros(posts_size, dtype=np.int32)
tag_index = np.zeros((last_tag + 1) * 2, dtype=np.int32)

cur = 0
for tag_name, data in tqdm(tag_dict.items()):
    # Insert all post ids for tag_id
    tag_index[2 * data[0]] = cur
    cur += data[1]
    tag_index[(2 * data[0]) + 1] = (cur - 1)

# Insert post IDs
prev_id = 0
for post_id, tags in tqdm(post_list):
    prev_id = post_id
    for tag in tags:
        data = tag_dict[tag]

        # data is [id, post_count, current_offset]
        # id finds the offset
        start = tag_index[2 * data[0]]
        posts[start + data[2]] = post_id
        data[2] += 1

with tags_posts.open("xb") as dest:
    np.save(dest, posts)

with index.open("xb") as dest:
    np.save(dest, tag_index)
