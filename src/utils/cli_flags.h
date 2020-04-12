#pragma once

#include <iostream>
#include <string>
#include <unordered_map>

using namespace std;

/**
 * a class for parsing args from the command line
 */
class CliFlags {
private:
  unordered_map<string, optional<string>> flags;

public:
  /**
   * add a flag for the parser to look for
   */
  CliFlags &add_flag(const string &flag) {
    flags.emplace(flag, nullopt);
    return *this;
  }

  /**
   * parse the given inputs as the flags which have been added by add_flag
   */
  void parse(int argc, char **argv, bool print = false) {
    if (print) {
      cout << "called:";
      for (int i = 0; i < argc; i++)
        cout << " " << argv[i];
      cout << endl;
    }

    for (int i = 0; i < argc;) {
      auto it = flags.find(argv[i]);
      if (it != flags.end() && i + 1 < argc) {
        flags.insert_or_assign(it, argv[i], string(argv[i + 1]));
        i += 2;
      } else {
        i++;
      }
    }
  }

  /**
   * get the value for a flag after the args have been parsed
   */
  optional<string> get_flag(const string &flag) const {
    if (flags.find(flag) != flags.end())
      return flags.at(flag);
    else
      return nullopt;
  }
};
