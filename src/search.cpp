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

namespace fs = std::filesystem;

static std::vector<int32_t> load_data(const fs::path& path) {
    std::ifstream infile(path, std::ios::in | std::ios::binary);
    if (!infile) {
        throw std::runtime_error{"couldn't open infile"};
    }

    /* Check magic number for raw array or numpy array */
    std::array<char, 6> magic;
    infile.read(magic.data(), magic.size());
    if (!infile) {
        throw std::runtime_error{"error reading magic string"};
    }

    bool is_numpy = (magic == std::array{'\x93', 'N', 'U', 'M', 'P', 'Y'});
    
    std::cerr << "Loading " << path.filename() << " as a " << (is_numpy ? "numpy" : "raw") << " file\n";

    infile.seekg(0, std::ios::end);
    std::streampos size = infile.tellg();

    /* Seek to start of array*/
    if (is_numpy) {
        infile.seekg(8, std::ios::beg);
        uint16_t start_offset;
        infile.read(reinterpret_cast<char*>(&start_offset), sizeof(start_offset));

        /* Subtract header size */
        size -= (start_offset + 8 + 2);

        infile.seekg(start_offset, std::ios::cur);
    } else {
        infile.seekg(0, std::ios::beg);
    }

    if (!infile) {
        throw std::runtime_error{"failed to seek to start of array"};
    }

    std::vector<int32_t> result(size / sizeof(int32_t));
    infile.read(reinterpret_cast<char*>(result.data()), result.size() * sizeof(decltype(result)::value_type));

    if (!infile) {
        throw std::runtime_error{"failed to read input array"};
    }

    return result;
}

static void usage(const char* argv0) {
    std::cerr << "Usage:\n\n"
              << argv0 << " <index_file> <tags_file>\n\n";

}

struct search_term {
    size_t start;
    size_t end;
    size_t cursor;
    //int32_t current;
};

std::vector<int32_t> search(std::span<int32_t> tags, std::span<search_term> search) {
    std::vector<int32_t> result;

    int32_t current_target = tags[search.front().cursor];

    for (;;) {
        /* Search for the next post that matches all terms */
        bool flag = true;

        /* Loop until next join or end */
        while (flag) {
            flag = false;
            size_t k = 0;
            for (search_term& term : search) {
                /* Join is found between `term` and current search target*/
                int32_t cur = tags[term.cursor];

                if (cur == current_target) {
                    ++k;
                    continue;
                }

                /* Try to find join */
                while (cur > current_target) {
                    if (term.cursor == term.start) {
                        /* End reached */
                        return result;
                    }

                    term.cursor -= 1;
                    cur = tags[term.cursor];
                }

                /* Join wasn't found, advance all to next */
                if (cur < current_target) {
                    current_target = cur;

                    for (size_t j = 0; j < k; ++j) {
                        if (search[j].cursor == search[j].start) {
                            /* Also end reached */
                            return result;
                        }

                        search[j].cursor -= 1;
                    }

                    flag = true;
                    break;
                }

                ++k;
            }
        }

        /* Current target matches all tags */
        result.push_back(current_target);

        /* Advance tags by 1, update next search target */
        for (search_term& term : search) {
            if (term.cursor == term.start) {
                /* Also end reached */
                return result;
            }

            term.cursor -= 1;

            int32_t cur = tags[term.cursor];
            if (cur < current_target) {
                current_target = cur;
            }
        }
    }
}

static void search_helper(std::span<int32_t> index, std::span<int32_t> tags, std::span<int32_t> search_ids) {
    std::vector<search_term> search_terms(search_ids.size());
    auto it = search_terms.begin();

    std::cerr << search_ids.size() << " tags:\n";
    for (int32_t tag_id : search_ids) {
        it->start = index[tag_id * 2],
        it->end = index[(tag_id * 2) + 1],
        it->cursor = it->end;
        //it->current = tags[it->cursor];

        std::cerr << "  tag " << tag_id << ": start=" << it->start << ", end=" << it->end << '\n';

        ++it;
    }

    std::cerr << '\n';

    static constexpr size_t repeats = 1000;
    
    std::vector<int32_t> results;

    auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < repeats; ++i) {
        auto search_terms_copy = search_terms;
        results = search(tags, search_terms_copy);
    }
    auto end = std::chrono::steady_clock::now();

    auto elapsed = end - start;
    std::cerr << "Found " << results.size() << " results in "
              << std::setprecision(6) << std::fixed << (elapsed.count() / 1e9 / repeats) << " seconds average ("
              << (elapsed.count() / 1e9) << " seconds total for " << repeats << " iterations)"
              << (results.empty() ? "" : ":") << '\n';

    std::ranges::sort(results);

    if (!results.empty()) {
        for (size_t i = 0; i < (results.size() - 1); ++i) {
            std::cerr << results[i] << ", ";
        }

        std::cerr << results.back() << '\n';
    }

    std::cerr << "\n\n";
}

int main(int argc, char** argv) {
    if (argc != 3) {
        usage(*argv);
        return EXIT_FAILURE;
    }

    fs::path index_path = argv[1];
    fs::path tags_path = argv[2];

    if (!exists(index_path) || !is_regular_file(index_path)) {
        std::cerr << "Index file does not exist or is not a file: " << index_path.string() << '\n';
        usage(*argv);
        return EXIT_FAILURE;
    }

    if (!exists(tags_path) || !is_regular_file(tags_path)) {
        std::cerr << "Tags file does not exist or is not a file: " << tags_path.string() << '\n';
        usage(*argv);
        return EXIT_FAILURE;
    }

    auto load_start = std::chrono::steady_clock::now();
    auto index = load_data(index_path);
    auto tags = load_data(tags_path);
    auto load_end = std::chrono::steady_clock::now();

    auto load_elapsed = load_end - load_start;
    std::cerr << "Loaded " << (index.size() / 2) << " tags and "
              << tags.size() << " post IDs in "
              << std::setprecision(6) << std::fixed << (load_elapsed.count() / 1e9) << " seconds\n\n";
    
    {
        std::array search_ids {
            470575, 212816, 13197, 29, 1283444, // 1girl solo long_hair touhou fate/grand_order
        };

        search_helper(index, tags, search_ids);
    }

    if (false) {
        std::array search_ids {
            1574450, 1665885, // t-doll_contract girls'_frontline
        };

        search_helper(index, tags, search_ids);
    }

    return EXIT_SUCCESS;
}
