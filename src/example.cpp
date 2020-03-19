#include "dataframe.h"
#include "parser.h"
#include "row.h"
#include "schema.h"
#include <iostream>

using namespace std;

int main(int argc, char **argv) {
  const char *filename = argc > 1 ? argv[1] : "datafile.sor";
  FILE *fd = fopen(filename, "rb");
  cout << "opened file " << fd << " name: " << filename << endl;
  if (fd == 0)
    return -1;
  fseek(fd, 0, SEEK_END);
  int file_len = ftell(fd);
  fseek(fd, 0, SEEK_SET);

  /* read file */
  /* TODO: chunk/stream this memory */
  char *bytes = new char[file_len];
  fread(bytes, file_len, 1, fd);
  fclose(fd);

  /* parse file */
  Parser p(file_len, bytes);
  Schema scm;
  p.infer_schema(scm);
  DataFrame df(scm);
  if (!p.parse_file(df)) {
    cerr << "Failed to parse dataframe" << endl;
    exit(-1);
  }

  delete[] bytes;

  /* fetch first num column from the DataFrame */
  int first_num_col;
  char col_type = '\0';
  for (int i = 0; i < df.ncols(); i++) {
    const Schema &scm = df.get_schema();
    if (scm.col_type(i) == Data::Type::INT ||
        scm.col_type(i) == Data::Type::FLOAT) {
      first_num_col = i;
      col_type = df.get_schema().col_type(i);
    }
  }
  if (col_type == Data::Type::INT) {
    cout << "Found int column, first 25 rows: \n| I |\n";
    for (int i = 0; i < 25; i++) {
      cout << "| ";
      if (i < df.nrows()) {
        cout << df.get<int>(first_num_col, i);
      } else {
        cout << " ";
      }
      cout << " |" << endl;
    }
  } else if (col_type == Data::Type::FLOAT) {
    cout << "Found float column, first 25 rows: \n| F |\n";
    for (int i = 0; i < 25; i++) {
      cout << "| ";
      if (i < df.nrows()) {
        cout << df.get<float>(first_num_col, i);
      } else {
        cout << "-";
      }
      cout << " |" << endl;
    }
  } else {
    cerr << "Failed to find num column to test" << endl;
  }

  /* sum every int in DataFrame */
  class SumIntRower : public Rower {
  public:
    double sum = 0;
    bool accept(const Row &r) {
      for (int i = 0; i < r.width(); i++) {
        if (r.is_missing(i))
          continue;
        if (r.get_schema().col_type(i) == Data::Type::INT) {
          sum += r.get<int>(i);
        } else if (r.get_schema().col_type(i) == Data::Type::FLOAT) {
          sum += r.get<float>(i);
        }
      }
      return true;
    }
  };
  SumIntRower rower;
  df.map(rower);
  cout << "Sum of all numbers in DF: " << rower.sum << endl;

  class StringDeleteRower : public Rower {
    bool accept(const Row &r) {
      for (int i = 0; i < r.width(); i++) {
        if (r.is_missing(i))
          continue;
        if (r.get_schema().col_type(i) == Data::Type::STRING) {
          delete r.get<string *>(i);
        }
      }
      return true;
    }
  };
  StringDeleteRower sdr;
  df.map(sdr);
}
