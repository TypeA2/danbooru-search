#ifndef HELPER_H
#define HELPER_H

#include <string>
#include <chrono>
#include <sstream>
#include <iomanip>

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

#endif /* HELPER_H */
