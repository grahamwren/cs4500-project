#include "lib/simple_dataframe.h"
#include "sdk/parser.h"
#include <chrono>
#include <iostream>

using namespace std;

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
