#include <iostream>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <set>
#include <cmath>
#include <functional>
#include <unordered_map>
#include <ranges>

#include <simdjson.h>

#include "helper.hpp"

namespace fs = std::filesystem;

using namespace std::literals;

struct tag_descriptor {
    uint32_t id;
    std::vector<uint32_t> posts;
};

struct string_hash {
    using hash_type = std::hash<std::string_view>;
    using is_transparent = void;

    std::size_t operator()(std::string_view sv) const { return hash_type{}(sv); }
    std::size_t operator()(const std::string& s) const { return hash_type{}(s); }
};

using tag_map_t = std::unordered_map<std::string, tag_descriptor, string_hash, std::equal_to<>>;

using tag_id_map_t = std::unordered_map<uint32_t, std::vector<uint32_t>>;

static void usage(const char* argv0) {
    std::cerr << "Usage:\n\n"
              << argv0 << " <index_file> <tags_file>\n\n";

}

tag_map_t read_tags(simdjson::ondemand::parser& parser, const fs::path& tags_json) {
    std::ifstream tags_file { tags_json };
    if (!tags_file) {
        std::cerr << "Failed opening tags.json\n";
        std::exit(EXIT_FAILURE);
    }

    static constexpr size_t tag_count = 1138080;
    tag_map_t tags;

    auto tag_start = std::chrono::steady_clock::now();

    size_t tag_bytes_read = 0;

    progress_bar progress{ "Reading tags", tag_count };
    for (std::string line; std::getline(tags_file, line);) {
        tag_bytes_read += line.size();

        simdjson::padded_string padded = line;
        auto doc = parser.iterate(padded);

        tag_descriptor desc {
            .id = static_cast<uint32_t>(doc["id"].get_uint64()),
            .posts = {}
        };

        desc.posts.reserve(doc["post_count"].get_uint64());

        tags.insert({
            std::string { doc["name"].get_string().value() },
            std::move(desc)
        });

        progress.advance();
    }

    progress.finish();

    auto tag_end = std::chrono::steady_clock::now();
    auto tag_read_elapsed = tag_end - tag_start;

    std::cerr << "Read " << tags.size() << " tags in " << get_time(tag_read_elapsed) << " ("
              << get_bytes(tag_bytes_read / (tag_read_elapsed.count() / 1e9)) << "/s)\n";

    return tags;
}

uint32_t read_posts(simdjson::ondemand::parser& parser, const fs::path& posts_json,
                tag_map_t& tags) {

    std::ifstream posts_file { posts_json };
    if (!posts_file) {
        std::cerr << "Failed opening posts.json\n";
        std::exit(EXIT_FAILURE);
    }

    auto post_start = std::chrono::steady_clock::now();

    static constexpr size_t post_count = 6196347;

    size_t post_bytes_read = 0;
    size_t posts_read = 0;
    uint32_t max_post = 0;
    progress_bar progress("Reading posts", post_count);
    for (std::string line; std::getline(posts_file, line);) {
        post_bytes_read += line.size();

        simdjson::padded_string padded = line;
        auto doc = parser.iterate(padded);

        uint32_t id = doc["id"].get_uint64();

        if (id > max_post) {
            max_post = id;
        }

        std::string_view tag_string = doc["tag_string"].get_string();

        for (const auto tag_subrange : std::views::split(tag_string, ' ')) {
            std::string_view tag { tag_subrange.begin(), tag_subrange.end() };

            auto it = tags.find(tag);
            if (it != tags.end()) {
                it->second.posts.push_back(id);
            } else {
                throw std::runtime_error("unknown tag: "s + std::string(tag.begin(), tag.end()));
            }
        }

        progress.advance();
        ++posts_read;
    }

    progress.finish();

    auto post_end = std::chrono::steady_clock::now();
    auto post_read_elapsed = post_end - post_start;

    std::cerr << "Read " << posts_read << " posts in " << get_time (post_read_elapsed) << " ("
              << get_bytes(post_bytes_read / (post_read_elapsed.count() / 1e9)) << "/s)\n";

    return max_post;
}

template <std::integral T>
struct bin_helper {
    T val;
};

template <std::integral T>
auto binary(T val) {
    return bin_helper { .val = val };
}

template <std::integral T>
std::ostream& operator<<(std::ostream& os, bin_helper<T> val) {
    return os.write(reinterpret_cast<char*>(&val.val), sizeof(T));
}

int main(int argc, char** argv) {
    if (argc != 2) {
        usage(*argv);
        return EXIT_FAILURE;
    }

    fs::path data_dir = argv[1];
    fs::path tags_json = data_dir / "tags.json";
    fs::path posts_json = data_dir / "posts.json";
    fs::path out_bin = data_dir / "index.bin";

    if (!exists(tags_json) || !is_regular_file(tags_json)) {
        std::cerr << "tags.json does not exist or is not a file: " << tags_json.string() << '\n';
        usage(*argv);
        return EXIT_FAILURE;
    }

    if (!exists(posts_json) || !is_regular_file(posts_json)) {
        std::cerr << "posts.json does not exist or is not a file: " << posts_json.string() << '\n';
        usage(*argv);
        return EXIT_FAILURE;
    }

    std::ofstream outfile(out_bin, std::ios::out | std::ios::binary);
    if (!outfile) {
        std::cerr << "Failed to open index.bin\n";
        return EXIT_FAILURE;
    }

    simdjson::ondemand::parser parser;

    tag_map_t tag_map = read_tags(parser, tags_json);
    uint32_t max_post = read_posts(parser, posts_json, tag_map);

    /* Re-shape tag map since we don't need tag names anymore */
    tag_id_map_t id_map;

    /* Also find higehst tag number */
    uint32_t max_tag = 0;
    for (const auto& [name, desc] : tag_map) {
        if (desc.id > max_tag) {
            max_tag = desc.id;
        }

        id_map.insert({ desc.id, std::move(desc.posts) });
    }

    auto write_start = std::chrono::steady_clock::now();

    progress_bar progress("Writing index", max_tag);

    size_t bytes_written = 4 + 4 + 4;
    size_t posts_written = 0;

    /* Magic number */
    outfile.write("Awoo", 4);

    /* All 32-bit ints */
    
    /* Highest post numer */
    outfile << binary<uint32_t>(max_post);

    /* Tag count is the highest tag number */
    outfile << binary<uint32_t>(max_tag);

    /* Post count for every tag number */
    for (uint32_t i = 0; i < max_tag; ++i) {
        auto it = id_map.find(i);
        if (it == id_map.end()) {
            /* No posts */
            outfile << binary<uint32_t>(0);
        } else {
            outfile << binary<uint32_t>(it->second.size());
        }

        bytes_written += 4;
    }

    /* Posts for every tag that exists */
    for (uint32_t i = 0; i < max_tag; ++i) {
        auto it = id_map.find(i);
        if (it != id_map.end()) {
            posts_written += it->second.size();

            const size_t bytes = it->second.size() * sizeof(uint32_t);
            outfile.write(reinterpret_cast<char*>(it->second.data()), bytes);

            bytes_written += bytes;
        }

        progress.advance();
    }
    progress.finish();

    auto write_elapsed = std::chrono::steady_clock::now() - write_start;

    std::cerr << "Wrote " << get_bytes(bytes_written) << ", " << max_tag << " post counts, "
              << posts_written << " posts in " << get_time(write_elapsed)
              << " (" << get_bytes(bytes_written / (write_elapsed.count() / 1e9)) << "/s)\n";
}
