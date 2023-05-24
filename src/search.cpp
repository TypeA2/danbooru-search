#include <iostream>
#include <vector>
#include <fstream>
#include <string>
#include <array>
#include <filesystem>
#include <span>
#include <chrono>
#include <iomanip>
#include <algorithm>
#include <set>
#include <optional>
#include <limits>

#include "helper.hpp"

namespace fs = std::filesystem;

using index_t = std::vector<std::vector<uint32_t>>;

static index_t load_index(const fs::path& path) {
    std::ifstream infile(path, std::ios::in | std::ios::binary);
    if (!infile) {
        throw std::runtime_error{"couldn't open index file"};
    }

    std::cerr << "Loading " << path.filename() << '\n';

    auto begin = std::chrono::steady_clock::now();

    /* Check magic number for raw array or numpy array */
    std::array<char, 4> magic;
    infile.read(magic.data(), magic.size());
    if (!infile || magic != std::array{'A', 'w', 'o', 'o'}) {
        throw std::runtime_error{"error reading magic string"};
    }

    size_t total_posts = 0;

    uint32_t max_post = 0;
    infile.read(reinterpret_cast<char*>(&max_post), sizeof(max_post));

    uint32_t tag_count = 0;
    infile.read(reinterpret_cast<char*>(&tag_count), sizeof(tag_count));

    index_t result(tag_count);
    for (std::vector<uint32_t>& tag : result) {
        uint32_t post_count = 0;
        infile.read(reinterpret_cast<char*>(&post_count), sizeof(post_count));

        if (post_count == 0) {
            continue;
        }

        tag.resize(post_count);

        total_posts += post_count;
    }

    for (std::span<uint32_t> tag : result) {
        infile.read(reinterpret_cast<char*>(tag.data()), tag.size_bytes());
    }

    auto elapsed = std::chrono::steady_clock::now() - begin;

    const size_t total_bytes = 4 + 4 + (tag_count * sizeof(uint32_t)) + (total_posts * sizeof(uint32_t));

    std::cerr << "Read " << tag_count << " tags, " << total_posts << " posts (up to ID " << max_post << "), "
        << get_bytes(total_bytes) << " in " << get_time(elapsed) << " (" << get_bytes(total_bytes / (elapsed.count() / 1e9)) << "/s)\n";

    return result;
}

static void usage(const char* argv0) {
    std::cerr << "Usage:\n\n"
              << argv0 << " <index_file>\n\n";

}

std::vector<uint32_t> search(index_t& index, std::vector<uint32_t> search_ids) {
    std::ranges::sort(search_ids, [&index](uint32_t lhs, uint32_t rhs ) {
        return index.at(lhs).size() < index.at(rhs).size();
    });

    std::vector<uint32_t> result;

    std::vector<uint32_t> cursor(search_ids.size(), 0);

    /* Search from least to most populated tag */
    for (uint32_t next_post : index[search_ids.front()]) {
        bool no_join = false;
        uint32_t i = 1;
        for (; i < search_ids.size(); ++i) {
            /* For every post in every remaining search id */
            std::span<uint32_t> posts = index[search_ids[i]];
            uint32_t j = cursor[i];
            for (; j < posts.size(); ++j) {
                uint32_t post = posts[j];

                if (post >= next_post) {
                    no_join = (post > next_post);
                    break;
                }
            }

            cursor[i] = j;

            if (no_join) {
                break;
            } else if (j == posts.size()) {
                /* Reached end, no more joins possible */
                return result;
            }
        }

        if (i == search_ids.size()) {
            /* Join on all! */
            result.push_back(next_post);
        }
    }

    return result;
}

static void search_helper(index_t& index, std::span<uint32_t> search_ids, std::optional<std::span<uint32_t>> expected = {}) {
    std::cerr << search_ids.size() << " tags to search:\n";
    for (uint32_t tag_id : search_ids) {
        std::cerr << "  " << tag_id << " -> " << index.at(tag_id).size() << " posts\n";
    }
    std::cerr << '\n';

    static constexpr size_t repeats = 100;
    
    std::vector<uint32_t> results;
    auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < repeats; ++i) {
        results = search(index, { search_ids.begin(), search_ids.end() });
    }
    auto end = std::chrono::steady_clock::now();

    auto elapsed = end - start;
    std::cerr << "Found " << results.size() << " results in "
              << get_time(elapsed / repeats) << " average ("
              << get_time(elapsed) << " total for " << repeats << " iterations)\n";

    if (!expected.has_value()) {
        std::cerr << '\n';
        return;
    }

    std::ranges::sort(results);

    std::set<int32_t> actual_set { results.begin(), results.end() };
    std::set<int32_t> expected_set { expected.value().begin(), expected.value().end() };

    if (actual_set != expected_set) {
        std::cerr << "  Results do not match expected results:\n";

        std::set<int32_t> extra_actual;
        std::set_difference(actual_set.begin(), actual_set.end(), expected_set.begin(), expected_set.end(), std::inserter(extra_actual, extra_actual.begin()));
        if (!extra_actual.empty()) {
            std::cerr << "    Additional found posts:\n";
            for (int32_t v : extra_actual) {
                std::cerr << "     - " << v << '\n';
            }
            std::cerr << '\n';
        }

        std::set<int32_t> missing_actual;
        std::set_difference(expected_set.begin(), expected_set.end(), actual_set.begin(), actual_set.end(), std::inserter(missing_actual, missing_actual.begin()));

        if (!missing_actual.empty()) {
            std::cerr << "    Missing posts:\n";
            for (int32_t v : missing_actual) {
                std::cerr << "     - " << v << '\n';
            }
        }
    } else {
        std::cerr << "  Results match expected results\n";
    }

    std::cerr << '\n';
}

int main(int argc, char** argv) {
    if (argc != 2) {
        usage(*argv);
        return EXIT_FAILURE;
    }

    fs::path index_path = argv[1];

    if (!exists(index_path) || !is_regular_file(index_path)) {
        std::cerr << "Index file does not exist or is not a file: " << index_path.string() << '\n';
        usage(*argv);
        return EXIT_FAILURE;
    }

    index_t index = load_index(index_path);
    
    {
        std::array<uint32_t, 5> search_ids {
            470575, 212816, 13197, 29, 1283444, // 1girl solo long_hair touhou fate/grand_order
        };

        std::array<uint32_t, 17> expected {
            2380549, 2420287, 2423105, 2523394, 2646037,
            2683860, 2705783, 2745868, 2746265, 2752461,
            2905088, 2917346, 3114201, 4081318, 4718669,
            5639802, 6055186
        };

        search_helper(index, search_ids, expected);
    }

    {
        std::array<uint32_t, 2> search_ids {
            1574450, 1665885, // t-doll_contract girls'_frontline
        };

        search_helper(index, search_ids);
    }

    return EXIT_SUCCESS;
}
