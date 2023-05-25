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
#include <climits>
#include <variant>
#include <unordered_map>
#include <ranges>

#include "helper.hpp"

namespace fs = std::filesystem;

/* https://en.cppreference.com/w/cpp/utility/variant/visit */
template<class... Ts>
struct overloaded : Ts... { using Ts::operator()...; };

using mask_val_t = __uint128_t;
static constexpr size_t MASK_SIZE = sizeof(mask_val_t) * CHAR_BIT;

struct mask_desc {
    uint32_t post_count;
    std::vector<mask_val_t> mask;

    mask_desc() = default;
    mask_desc(mask_desc&&) noexcept = default;
    mask_desc(const mask_desc&) = delete;
    mask_desc(uint32_t post_count, size_t mask_count)
        : post_count { post_count }, mask(mask_count, 0) { }

    mask_desc& operator=(mask_desc&&) noexcept = default;
    mask_desc& operator=(const mask_desc&) = default;

    mask_val_t& operator[](size_t idx) { return mask[idx]; }
    const mask_val_t& operator[](size_t idx) const { return mask[idx]; }
};
using index_value_t = std::variant<std::monostate, std::vector<uint32_t>, mask_desc>;

struct post_index {
    uint32_t max_post;
    std::vector<index_value_t> data;

    [[nodiscard]] size_t mask_size() const { return ((max_post + MASK_SIZE - 1) / MASK_SIZE); }
    [[nodiscard]] const index_value_t& at(size_t idx) const { return data.at(idx); }
    [[nodiscard]] index_value_t& operator[](size_t idx) { return data[idx]; }
    [[nodiscard]] size_t size() const { return data.size(); }
};

using index_t = post_index;

/* Minimum number of posts before we use a tag instead */
static constexpr size_t MASK_THRESHOLD = 50'000;

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

    index_t result;

    infile.read(reinterpret_cast<char*>(&result.max_post), sizeof(result.max_post));

    uint32_t tag_count = 0;
    infile.read(reinterpret_cast<char*>(&tag_count), sizeof(tag_count));

    result.data.resize(tag_count);

    size_t mask_tags = 0;

    std::unordered_map<uint32_t, uint32_t> mask_sizes;

    size_t index_bytes = 0;

    progress_bar p0 { "Reading tags", result.size() };
    for (uint32_t i = 0; i < result.size(); ++i) {
        uint32_t post_count = 0;
        infile.read(reinterpret_cast<char*>(&post_count), sizeof(post_count));

        if (post_count == 0) {
            continue;
        }

        if (post_count >= MASK_THRESHOLD) {
            result[i] = mask_desc(post_count, result.mask_size());

            index_bytes += sizeof(mask_val_t)* result.mask_size();
            mask_sizes.insert({ i, post_count });
        } else {
            result[i] = std::vector<uint32_t>(post_count, 0);
            index_bytes += post_count * sizeof(uint32_t);
        }

        total_posts += post_count;

        p0.advance();
    }

    p0.finish();

    progress_bar p1 { "Reading posts", result.size() };
    for (uint32_t i = 0; i < result.size(); ++i) {
        index_value_t& tag = result[i];

        std::visit(overloaded {
            /* Empty */
            [](std::monostate) { },
            
            /* Raw post IDs */
            [&](std::span<uint32_t> posts) {
                infile.read(reinterpret_cast<char*>(posts.data()), posts.size_bytes());
            },

            /* Bitmask*/
            [&](mask_desc& mask) {
                /* Read in the usual 4KiB chunks */
                ssize_t posts_remaining = mask_sizes.at(i);

                mask_tags += 1;

                while (posts_remaining > 0) {
                    std::array<uint32_t, 4096/sizeof(uint32_t)> buf;

                    const uint32_t posts_to_read = std::min<uint32_t>(posts_remaining, buf.size());
                    infile.read(reinterpret_cast<char*>(buf.data()), posts_to_read * sizeof(uint32_t));
                    posts_remaining -= posts_to_read;

                    /* Set every bit corresponding to a post */
                    for (uint32_t j = 0; j < posts_to_read; ++j) {
                        uint32_t index = buf[j] / MASK_SIZE;
                        uint32_t offset = buf[j] % MASK_SIZE;

                        mask[index] |= mask_val_t{1} << offset;
                    }
                }
            }
        }, tag);
        
        p1.advance();
    }

    p1.finish();

    auto elapsed = std::chrono::steady_clock::now() - begin;

    const size_t total_bytes = 4 + 4 + (tag_count * sizeof(uint32_t)) + (total_posts * sizeof(uint32_t));

    std::cerr << "Read " << (tag_count - mask_tags) << " tags, "
        << mask_tags << " masked tags, "
        << total_posts << " posts (up to ID " << result.max_post << "), "
        << get_bytes(index_bytes) << " total memory, "
        << get_bytes(total_bytes) << " in " << get_time(elapsed) << " (" << get_bytes(total_bytes / (elapsed.count() / 1e9)) << "/s)\n";

    return result;
}

static void usage(const char* argv0) {
    std::cerr << "Usage:\n\n"
              << argv0 << " <index_file>\n\n";

}

std::vector<uint32_t> search(index_t& index, std::vector<uint32_t> search_ids) {
    std::ranges::sort(search_ids, [&index](uint32_t lhs, uint32_t rhs) {
        static overloaded visitor {
            [](std::monostate) -> size_t { return 0; },
            [](const std::vector<uint32_t>& ids) -> size_t { return ids.size(); },

            /* May not represent the exact size, but since we always process MASK_SIZE
             * bits this is irrelevant
             */
            [](const mask_desc& mask) -> size_t { return mask.post_count; }
        };

        size_t lhs_size = std::visit(visitor, index.at(lhs));
        size_t rhs_size = std::visit(visitor, index.at(rhs));

        return lhs_size < rhs_size;
    });

    std::vector<mask_val_t> result_mask(index.mask_size(), 0);

    std::visit(overloaded {
        [](std::monostate) { },

        [&result_mask](const std::vector<uint32_t>& ids) {
            for (uint32_t id : ids) {
                uint32_t index = id / MASK_SIZE;
                uint32_t offset = id % MASK_SIZE;

                result_mask[index] |= mask_val_t{1} << offset;
            }
        },

        [&result_mask](const mask_desc& mask) {
            std::ranges::copy(mask.mask, result_mask.data());
        },

    }, index.at(search_ids.front()));

    /* Effectively bitwise AND on the result_mask */
    overloaded visitor {
        [](std::monostate) { },

        [&result_mask](const std::vector<uint32_t>& ids) {
            for (uint32_t id : ids) {
                uint32_t index = id / MASK_SIZE;
                uint32_t offset = id % MASK_SIZE;

                mask_val_t& current = result_mask[index];
                if (current) {
                    current &= mask_val_t{1} << offset;
                }
            }
        },

        [&result_mask](const mask_desc& mask) {
            for (size_t i = 0; i < result_mask.size(); ++i) {
                mask_val_t& current = result_mask[i];
                if (current) {
                    current &= mask[i];
                }
            }
        },
    };

    for (uint32_t id : search_ids | std::views::drop(1)) {
        /* TODO early exit when nothing is found */
        std::visit(visitor, index.at(id));
    }

    std::vector<uint32_t> results;

    uint32_t idx = 0;
    for (mask_val_t mask : result_mask) {

#if 0
        while (mask) {
            int trailing = __builtin_ctzll(mask);

            results.push_back(idx + trailing);
            mask &= ~(mask_val_t{1} << trailing);
        }

        idx += MASK_SIZE;

#else

        if (mask) {
            for (uint32_t i = 0; i < MASK_SIZE; ++i, ++idx) {
                if (mask & (mask_val_t{1} << i)) {
                    results.push_back(idx);
                }
            }
        } else {
            idx += MASK_SIZE;
        }
#endif
    }

    return results;
}

static void search_helper(index_t& index, std::span<uint32_t> search_ids, std::optional<std::span<uint32_t>> expected = {}) {
    static constexpr size_t repeats = 1'000;
    
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
