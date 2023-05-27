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
#include <compare>
#include <bitset>
#include <immintrin.h>

#include "helper.hpp"
#include "simd.hpp"
#include "avx_buffer.hpp"

using namespace simd::epi32_operators;
namespace epi32 = simd::epi32;

namespace fs = std::filesystem;

/* https://en.cppreference.com/w/cpp/utility/variant/visit */
template<class... Ts>
struct overloaded : Ts... { using Ts::operator()...; };

using mask_val_t = __uint128_t;
static constexpr size_t MASK_SIZE = sizeof(mask_val_t) * CHAR_BIT;

struct mask_desc {
    uint32_t post_count;
    avx_buffer<mask_val_t> mask;

    mask_desc() = default;
    mask_desc(mask_desc&&) noexcept = default;
    mask_desc(const mask_desc&) = delete;
    mask_desc(uint32_t post_count, size_t mask_count)
        : post_count { post_count }, mask(avx_buffer<mask_val_t>::zero(mask_count)) { }

    mask_desc& operator=(mask_desc&&) noexcept = default;
    mask_desc& operator=(const mask_desc&) = default;

    mask_val_t& operator[](size_t idx) { return mask[idx]; }
    const mask_val_t& operator[](size_t idx) const { return mask[idx]; }
};
using index_value_t = std::variant<std::monostate, std::vector<uint32_t>, mask_desc>;

static overloaded sort_visitor {
    [](std::monostate) -> size_t { return 0; },
    [](const std::vector<uint32_t>& ids) -> size_t { return ids.size(); },
    [](const mask_desc& mask) -> size_t { return mask.post_count; }
};

struct post_index {
    uint32_t max_post;
    std::vector<index_value_t> data;

    [[nodiscard]] size_t mask_size() const { return ((max_post + MASK_SIZE - 1) / MASK_SIZE); }
    [[nodiscard]] const index_value_t& at(size_t idx) const { return data.at(idx); }
    [[nodiscard]] index_value_t& operator[](size_t idx) { return data[idx]; }
    [[nodiscard]] size_t size() const { return data.size(); }
};

using index_t = post_index;

static constexpr size_t repeats = 1'000;

struct timekeeping {
    using duration = std::chrono::steady_clock::duration;
    duration sort;
    duration initialize;
    duration mask;
    duration result;

    duration avg_sort() const { return sort / repeats; }
    duration avg_initialize() const { return initialize / repeats; }
    duration avg_mask() const { return mask / repeats; }
    duration avg_result() const { return result / repeats; }

    duration total() const { return sort + initialize + mask + result; }
    duration avg_total() const { return total() / repeats; }
};

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

    std::unordered_map<uint32_t, uint32_t> mask_sizes;

    size_t index_bytes = 0;

    size_t masks = 0;
    size_t id_lists = 0;

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

            ++masks;
        } else {
            result[i] = std::vector<uint32_t>(post_count, 0);
            index_bytes += post_count * sizeof(uint32_t);

            ++id_lists;
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

    std::cerr << "Read " << tag_count << " tags, "
        << total_posts << " posts (up to ID " << result.max_post << ")\n"
        << "  " << (tag_count - id_lists - masks) << " empty tags, " << id_lists << " ID lists, " << masks << " mask arrays ("
        << get_bytes(sizeof(mask_val_t) * result.mask_size()) << " per mask)\n"
        << "  " << get_bytes(index_bytes) << " total memory, "
        << get_bytes(total_bytes) << " in " << get_time(elapsed) << " (" << get_bytes(total_bytes / (elapsed.count() / 1e9)) << "/s)\n\n";

    return result;
}

static void usage(const char* argv0) {
    std::cerr << "Usage:\n\n"
              << argv0 << " <index_file>\n\n";

}

std::vector<uint32_t> search(timekeeping& trace, index_t& index, std::vector<uint32_t> search_ids) {
    auto a = std::chrono::steady_clock::now();
    std::ranges::sort(search_ids, [&index](uint32_t lhs, uint32_t rhs) {
        size_t lhs_size = std::visit(sort_visitor, index.at(lhs));
        size_t rhs_size = std::visit(sort_visitor, index.at(rhs));

        return lhs_size < rhs_size;
    });

    auto b = std::chrono::steady_clock::now();

    //std::vector<mask_val_t> result_mask(index.mask_size(), 0);
    auto result_mask = avx_buffer<mask_val_t>::zero(index.mask_size());
#if 0
    overloaded initialize_visitor {
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

    };

    std::visit(initialize_visitor, index.at(search_ids.front()));
    
    static constexpr size_t drop_count = 1;
    
#else

    /* Separately combine the first two */
    overloaded first_tag_visitor {
        [](std::monostate, std::monostate) { },
        [](std::monostate, const std::vector<uint32_t>&) { },
        [](std::monostate, const mask_desc&) { },
        [](const std::vector<uint32_t>&, std::monostate) { },
        [](const mask_desc&, std::monostate) { },

        [&result_mask](const std::vector<uint32_t>& lhs, const std::vector<uint32_t>& rhs) {
            auto left_it = lhs.begin();
            auto right_it = rhs.begin();

            while (left_it != lhs.end() && right_it != rhs.end()) {
                auto order = *left_it <=> *right_it;

                if (order == std::strong_ordering::less) {
                    ++left_it;
                } else if (order == std::strong_ordering::greater) {
                    ++right_it;
                } else {
                    uint32_t index = *left_it++ / MASK_SIZE;
                    uint32_t offset = *right_it++ % MASK_SIZE;

                    result_mask[index] |= mask_val_t{1} << offset;
                }
            }
        },

        [&result_mask](const std::vector<uint32_t>& lhs, const mask_desc& rhs) {
            for (uint32_t id : lhs) {
                uint32_t index = id / MASK_SIZE;
                uint32_t offset = id % MASK_SIZE;

                auto mask = mask_val_t{1} << offset;
                result_mask[index] |= mask & rhs[index];
            }
        },

        [&result_mask](const mask_desc& lhs, const std::vector<uint32_t>& rhs) {
            for (uint32_t id : rhs) {
                uint32_t index = id / MASK_SIZE;
                uint32_t offset = id % MASK_SIZE;

                auto mask = mask_val_t{1} << offset;
                result_mask[index] |= mask & lhs[index];
            }
        },

        [&result_mask](const mask_desc& lhs, const mask_desc& rhs) {
            for (size_t i = 0; i < result_mask.size_m256i(); ++i) {
                //result_mask[i] = lhs[i] & rhs[i];
                //mask_val_t res = lhs[i] & rhs[i];
                //if (res) {
                //    result_mask[i] = res;
                //}
                m256i l = _mm256_load_si256(lhs.mask.m256i(i));
                m256i r = _mm256_load_si256(rhs.mask.m256i(i));

                m256i result = l & r;
                _mm256_store_si256(result_mask.m256i(i), result);
            }
        },
    };

    std::visit(first_tag_visitor, index.at(search_ids[0]), index.at(search_ids[1]));

    static constexpr size_t drop_count = 2;

#endif

    auto c = std::chrono::steady_clock::now();

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
            for (size_t i = 0; i < result_mask.size_m256i(); ++i) {
                m256i next = _mm256_load_si256(mask.mask.m256i(i));

                if (!epi32::is_zero(next)) {
                    m256i cur = _mm256_load_si256(result_mask.m256i(i));

                    m256i result = cur & next;

                    epi32::store(result_mask.m256i(i), result);
                }
            }
            //for (size_t i = 0; i < result_mask.size(); ++i) {
                //mask_val_t& current = result_mask[i];
                //if (current) {
                    //current &= mask[i];
                //}
            //}
        },
    };

#define DROP_END 1

    for (size_t i = drop_count; i < (search_ids.size() - DROP_END); ++i) {
        std::visit(visitor, index.at(search_ids[i]));
    }

    auto d = std::chrono::steady_clock::now();

    std::vector<uint32_t> results;

#if DROP_END == 0
    uint32_t idx = 0;
    for (mask_val_t mask : result_mask) {

#if 0

        for (size_t i = 0; i < (MASK_SIZE / 64); ++i) {
            uint64_t submask = (mask >> (i * 64)) & 0xFFFFFFFFFFFFFFFFull;
            while (submask) {
                int trailing = __builtin_ctzll(submask);

                results.push_back(idx + trailing);
                submask &= ~(uint64_t{1} << trailing);
            }

            idx += 64;
        }

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
#else

    /* Perform last merge and result writing directly */
    overloaded final_visitor {
        [](std::monostate) { },

        [&results, &result_mask](const std::vector<uint32_t>& ids) {
            for (uint32_t id : ids) {
                uint32_t index = id / MASK_SIZE;
                uint32_t offset = id % MASK_SIZE;

                if (result_mask[index] & (mask_val_t{1} << offset)) {
                    results.push_back(id);
                }
            }
        },

        [&results, &result_mask](const mask_desc& masks) {
            for (size_t i = 0; i < result_mask.size(); ++i) {
                if (mask_val_t val = masks[i]; val) {
                    if (mask_val_t masked = result_mask[i] & val; masked) {
                        for (uint32_t j = 0; j < MASK_SIZE; ++j) {
                            if (masked & (mask_val_t{1} << j)) {
                                results.push_back((i * MASK_SIZE) + j);
                            }
                        }
                    }
                }
            }
        },
    };

    std::visit(final_visitor, index.at(search_ids.back()));

#endif
    auto e = std::chrono::steady_clock::now();

    trace.sort += (b - a);
    trace.initialize += (c - b);
    trace.mask += (d - c);
    trace.result += (e - d);

    return results;
}

static void search_helper(index_t& index, std::span<uint32_t> search_ids, std::optional<std::span<uint32_t>> expected = {}) {
    std::vector<uint32_t> sorted_search_ids { search_ids.begin(), search_ids.end() };
    std::ranges::sort(sorted_search_ids);

    for (uint32_t id : sorted_search_ids) {
        std::cerr << "Tag " << id << " -> ";

        switch (index.at(id).index()) {
            case 0: std::cerr << "unknown\n"; break;
            case 1: std::cerr << "ID list\n"; break;
            case 2: std::cerr << "Bitmask\n";  break;
        }
    }

    std::cerr << '\n';
    
    std::vector<uint32_t> results;

    timekeeping trace{};
    for (size_t i = 0; i < repeats; ++i) {
        results = search(trace, index, { search_ids.begin(), search_ids.end() });
    }

    std::cerr << "Found " << results.size() << " results in "
              << get_time(trace.avg_total()) << " average ("
              << get_time(trace.total()) << " total for " << repeats << " iterations)\n";

    std::cerr << "  Sort:         " << get_time(trace.avg_sort()) << '\n'
              << "  Initial mask: " << get_time(trace.avg_initialize()) << " ("
              << get_bytes((index.mask_size() * sizeof(mask_val_t)) / (trace.avg_initialize().count() / 1e9)) << "/s)\n"
              << "  Mask:         " << get_time(trace.avg_mask()) << '\n'
              << "  Read result:  " << get_time(trace.avg_result()) << '\n';

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
