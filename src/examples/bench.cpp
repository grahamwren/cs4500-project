#include "lib/simple_dataframe.h"
#include "sdk/parser.h"
#include <chrono>
#include <iostream>

using namespace std;

/**
 * Example benchmarking the parser against a file. Prints the length of the
 * file in bytes, the schema for the file, whether the parse was successful,
 * and the time it took to parse the file into a DataFrame in milliseconds.
 *
 * Benchmark uses a SimpleDataFrame which is a fully in-memory representation
 * of a DataFrame.
 * authors: @grahamwren, @jagen31
 */
int main(int argc, char **argv) {
  const char *filename = argc > 1 ? argv[1] : "datafile.sor";
  FILE *fd = fopen(filename, "r");
  cout << "opened file " << fd << " name: " << filename << endl;
  if (fd != 0) {
    fseek(fd, 0, SEEK_END);
    long length = ftell(fd);
    fseek(fd, 0, SEEK_SET);

    char *buf = new char[length];
    fread(buf, length, 1, fd);

    cout << "starting parsing len " << length << " file " << filename << endl;
    auto t1 = chrono::high_resolution_clock::now();

    Parser parser(length, buf);
    Schema scm;
    parser.infer_schema(scm);
    SimpleDataFrame df(scm);

    bool success = parser.parse_file(df);
    auto t2 = chrono::high_resolution_clock::now();

    char scm_buf[scm.width() + 1];
    scm.c_str(scm_buf);
    cout << "schema " << scm_buf << endl;
    if (success) {
      cout << "parse success for " << df.nrows() << " rows" << endl;
    } else {
      cout << "parse fail" << endl;
    }
    if (length < 2048) {
      df.print();
    }
    chrono::duration<double, milli> diff = t2 - t1;
    cout << diff.count() << " ms" << endl;
    fclose(fd);
  } else {
    cout << "Unknown file: " << filename << endl;
  }
  return 0;
}
