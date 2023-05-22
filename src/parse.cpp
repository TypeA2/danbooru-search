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

#include <simdjson.h>

namespace fs = std::filesystem;

struct tag {
    uint64_t id;
    uint64_t post_count;
    std::string name;
};

struct post {
    uint64_t id;
    std::string tag_string;
    std::set<std::string_view> tags;
};

class progress_bar {
    std::string_view _prompt;
    const uint64_t _max;
    const double _maxf;
    const uint64_t _step;

    uint64_t _cur = 0;
    uint64_t _last_update = 0;

    public:
    progress_bar(std::string_view prompt, uint64_t max)
        : _prompt { prompt }
        , _max { max }
        , _maxf { static_cast<double>(max) }
        , _step { _max / 1000 } {

    }

    void advance(uint64_t n = 1) {
        _cur += n;

        if ((_cur - _last_update) > _step) {
            double percent = 100. * (_cur / _maxf);
            std::cout << '\r' << _prompt << ": " << std::setprecision(1) << std::fixed << percent << " %" << std::flush;
            _last_update = _cur;
        }
    }

    void finish() const {
        std::cout << '\r' << _prompt << ": 100.0 %\n";
    }
};

static void usage(const char* argv0) {
    std::cerr << "Usage:\n\n"
              << argv0 << " <index_file> <tags_file>\n\n";

}

std::string get_bytes(uint64_t n) {
    std::stringstream ss;
    ss << std::setprecision(3) << std::fixed;

    double val = n;
    if (n > size_t{1024}*1024*1024*1024) {
        val /= size_t{1024}*1024*1024*1024;
        ss << val << " TiB";
    } else if (n > 1024*1024*1024) {
        val /= 1024*1024*1024;
        ss << val << " GiB";
    } else if (n > 1024*1024) {
        val /= 1024*1024;
        ss << val << " MiB";
    } else if (n > 1024) {
        val /= 1024;
        ss << val << " KiB";
    } else {
        ss << val << " bytes";
    }

    return ss.str();
}

std::string get_time(std::chrono::nanoseconds ns) {
    auto n = ns.count();

    std::stringstream ss;
    ss << std::setprecision(3) << std::fixed;

    if (n > 1e9) {
        ss << (n / 1e9) << " seconds";
    } else if (n > 1e6) {
        ss << (n / 1e6) << " ms";
    } else if (n > 1e3) {
        ss << (n / 1e3) << " us";
    } else {
        ss << n << " ns";
    }

    return ss.str();
}

std::vector<tag> read_tags(simdjson::ondemand::parser& parser, const fs::path& tags_json) {
    std::ifstream tags_file { tags_json };
    if (!tags_file) {
        std::cerr << "Failed opening tags.json\n";
        std::exit(EXIT_FAILURE);
    }

    auto tag_start = std::chrono::steady_clock::now();
    static constexpr size_t tag_count = 1138080;
    std::vector<tag> tag_list;
    tag_list.reserve(tag_count);

    auto tag_alloc = std::chrono::steady_clock::now();

    size_t tag_bytes_read = 0;

    progress_bar progress{ "Reading tags", tag_count };
    for (std::string line; std::getline(tags_file, line);) {
        tag_bytes_read += line.size();

        simdjson::padded_string padded = line;
        auto doc = parser.iterate(padded);

        tag_list.push_back(tag {
            .id = doc["id"].get_uint64(),
            .post_count = doc["post_count"].get_uint64(),
            .name = std::string { doc["name"].get_string().value() },
        });

        progress.advance();
    }

    progress.finish();

    auto tag_end = std::chrono::steady_clock::now();
    auto tag_alloc_elapsed = tag_alloc - tag_start;
    auto tag_read_elapsed = tag_end - tag_alloc;

    std::cerr << "Allocated " << tag_count << " tags ("
              << get_bytes(sizeof(tag) * tag_count) << ") in " << get_time(tag_alloc_elapsed) << " ("
              << get_bytes((sizeof(tag) * tag_count) / (tag_alloc_elapsed.count() / 1e9)) << "/s)\n";
    
    std::cerr << "Read " << tag_list.size() << " tags in " << get_time(tag_read_elapsed) << " ("
              << get_bytes(tag_bytes_read / (tag_read_elapsed.count() / 1e9)) << "/s)\n";

    return tag_list;
}

std::vector<post> read_posts(simdjson::ondemand::parser& parser, const fs::path& posts_json) {
    std::ifstream posts_file { posts_json };
    if (!posts_file) {
        std::cerr << "Failed opening posts.json\n";
        std::exit(EXIT_FAILURE);
    }

    auto post_start = std::chrono::steady_clock::now();
    static constexpr size_t post_count = 6196347;
    std::vector<post> post_list;
    post_list.reserve(post_count);

    auto post_alloc = std::chrono::steady_clock::now();

    size_t post_bytes_read = 0;
    progress_bar progress("Reading posts", post_count);
    for (std::string line; std::getline(posts_file, line);) {
        post_bytes_read += line.size();

        simdjson::padded_string padded = line;
        auto doc = parser.iterate(padded);

        post post {
            .id = doc["id"].get_uint64(),
            .tag_string = std::string { doc["tag_string"].get_string().value() },
            .tags = {}
        };

        size_t pos = 0;
        size_t old_pos = 0;
        const char* start = post.tag_string.c_str();
        do {
            pos = post.tag_string.find(' ', old_pos);
            post.tags.emplace(start + old_pos, start + std::min<size_t>(pos, post.tag_string.size()));

            old_pos = pos + 1;
        } while (pos != std::string::npos);

        post_list.emplace_back(std::move(post));

        progress.advance();
    }

    progress.finish();

    auto post_end = std::chrono::steady_clock::now();
    auto post_alloc_elapsed = post_alloc - post_start;
    auto post_read_elapsed = post_end - post_alloc;

    std::cerr << "Allocated " << post_count  << " posts ("
              << get_bytes(sizeof(post) * post_count) << ") in " << get_time(post_alloc_elapsed) << " ("
              << get_bytes((sizeof(post) * post_count) / (post_alloc_elapsed.count() / 1e9)) << "/s)\n";

    std::cerr << "Read " << post_list.size() << " posts in " << get_time (post_read_elapsed) << " ("
              << get_bytes(post_bytes_read / (post_read_elapsed.count() / 1e9)) << "/s)\n";

    return post_list;
}

int main(int argc, char** argv) {
    if (argc != 2) {
        usage(*argv);
        return EXIT_FAILURE;
    }

    fs::path data_dir = argv[1];
    fs::path tags_json = data_dir / "tags.json";
    fs::path posts_json = data_dir / "posts.json";

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

    simdjson::ondemand::parser parser;

    auto tag_list = read_tags(parser, tags_json);
    auto post_list = read_posts(parser, posts_json);

    /* For every tag, make a list of the corresponding posts */
}
