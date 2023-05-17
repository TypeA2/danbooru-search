#!/usr/bin/env python3
# Parse posts.json and tags.json to pickled, usable formats

import sys
import time
import pickle
import newlinejson as nlj

from pathlib import Path
from tqdm import tqdm, trange

assert len(sys.argv) == 2, f"Usage:\n\n{sys.argv[0]} <data_dir>\n"

# Input parsing

data_dir = Path(sys.argv[1])
assert data_dir.exists() and data_dir.is_dir()

tags_json = data_dir / "tags.json"
posts_json = data_dir / "posts.json"

assert tags_json.exists() and tags_json.is_file()
assert posts_json.exists() and posts_json.is_file()

out_tags = tags_json.with_suffix(".pickle")
out_posts = posts_json.with_suffix(".pickle")

assert not out_tags.exists()
assert not out_posts.exists()

start = time.time()
# Pre-allocate tag list
TAG_COUNT = 1138080
tag_list = [()] * TAG_COUNT

i = 0
with nlj.open(str(tags_json)) as tags_src:
    for tag in tqdm(tags_src, total = TAG_COUNT):
        tag_list[i] = (tag["id"], tag["post_count"], tag["name"])
        i += 1
end = time.time()

print(f"{round(end - start, 3)} seconds to parse {len(tag_list)} tags")

start = time.time()
with out_tags.open("xb") as out:
    pickle.dump(tag_list, out, protocol=pickle.HIGHEST_PROTOCOL)
end = time.time()

print(f" > Pickled in {round(end - start, 3)} seconds")

start = time.time()
# Pre-allocate posts list
POST_COUNT = 6196347
post_list = [()] * POST_COUNT

i = 0
with nlj.open(str(posts_json)) as posts_src:
    for post in tqdm(posts_src, total=POST_COUNT):
        post_list[i] = (post["id"], post["tag_string"].split(" "))
        i += 1

end = time.time()
print(f"{round(end - start, 3)} seconds to parse {len(post_list)} posts")

start = time.time()
with out_posts.open("xb") as out:
    pickle.dump(post_list, out, protocol=pickle.HIGHEST_PROTOCOL)
end = time.time()

print(f" > Pickled in {round(end - start, 3)} seconds")
