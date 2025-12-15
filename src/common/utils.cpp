#include "common/utils.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

#ifdef _WIN32
#include <direct.h>
#include <sys/stat.h>
#define mkdir(path) _mkdir(path)
#define stat _stat
#else
#include <sys/stat.h>
#include <sys/types.h>
#endif

namespace dbms::pathutil {

std::string normalizePath(const std::string &path) {
    std::string result = path;
    for (char &ch : result) {
        if (ch == '\\') {
            ch = '/';
        }
    }
    return result;
}

std::string join(const std::string &lhs, const std::string &rhs) {
    if (lhs.empty()) {
        return rhs;
    }
    if (rhs.empty()) {
        return lhs;
    }
    if (lhs.back() == '/' || lhs.back() == '\\') {
        return lhs + rhs;
    }
    return lhs + "/" + rhs;
}

std::string parentDirectory(const std::string &path) {
    const std::string normalized = normalizePath(path);
    auto pos = normalized.find_last_of('/');
    if (pos == std::string::npos) {
        return "";
    }
    return normalized.substr(0, pos);
}

bool isDirectory(const std::string &path) {
    struct stat info;
    if (stat(path.c_str(), &info) != 0) {
        return false;
    }
    return (info.st_mode & S_IFDIR) != 0;
}

void ensureDirectory(const std::string &path) {
    std::string normalized = normalizePath(path);
    if (normalized.empty()) {
        return;
    }

    // Create directories recursively
    std::string current;
    for (size_t i = 0; i < normalized.size(); ++i) {
        if (normalized[i] == '/' || i == normalized.size() - 1) {
            if (i == normalized.size() - 1 && normalized[i] != '/') {
                current += normalized[i];
            }
            if (!current.empty() && !isDirectory(current)) {
                mkdir(current.c_str());
            }
        }
        current += normalized[i];
    }
}

void ensureParentDirectory(const std::string &filePath) {
    const std::string dir = parentDirectory(filePath);
    if (!dir.empty()) {
        ensureDirectory(dir);
    }
}

bool fileExists(const std::string &path) {
    struct stat info;
    return stat(path.c_str(), &info) == 0;
}

} // namespace dbms::pathutil

namespace dbms {

PersistentTextFile::PersistentTextFile(std::string path)
    : path_(std::move(path)) {
    pathutil::ensureParentDirectory(path_);
    std::ofstream file(path_, std::ios::app);
    if (!file) {
        std::ostringstream oss;
        oss << "failed to open persistence file: " << path_;
        throw std::runtime_error(oss.str());
    }
}

void PersistentTextFile::appendLine(const std::string &line) const {
    std::ofstream out(path_, std::ios::app);
    if (!out) {
        std::ostringstream oss;
        oss << "failed to append to persistence file: " << path_;
        throw std::runtime_error(oss.str());
    }
    out << line << '\n';
}

std::vector<std::string> PersistentTextFile::readAll() const {
    std::vector<std::string> lines;
    std::ifstream in(path_);
    if (!in) {
        return lines;
    }
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        lines.push_back(line);
    }
    return lines;
}

const std::string &PersistentTextFile::path() const {
    return path_;
}

} // namespace dbms
