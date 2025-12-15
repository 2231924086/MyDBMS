#pragma once

#include <string>
#include <vector>

namespace dbms::pathutil {

std::string normalizePath(const std::string &path);
std::string join(const std::string &lhs, const std::string &rhs);
std::string parentDirectory(const std::string &path);
bool isDirectory(const std::string &path);
void ensureDirectory(const std::string &path);
void ensureParentDirectory(const std::string &filePath);
bool fileExists(const std::string &path);

} // namespace dbms::pathutil

namespace dbms {

class PersistentTextFile {
public:
    explicit PersistentTextFile(std::string path);

    void appendLine(const std::string &line) const;
    std::vector<std::string> readAll() const;
    const std::string &path() const;

private:
    std::string path_;
};

} // namespace dbms
