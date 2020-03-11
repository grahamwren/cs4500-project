#include "dataframe.h"
#include "row.h"
#include "schema.h"
#include <iostream>

/* FakeParser */
class Parser {
public:
  Parser(int len, char *bytes) {}

  bool parse_file(DataFrame &df) { return false; }
}

using namespace std;

int main(int argc, char **argv) {
  const char *filename = argc > 1 ? argv[1] : "datafile.sor";
  FILE *fd = fopen(filename, "rb");
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
  DataFrame df;
  if (!p.parse_file(df)) {
    cerr << "Failed to parse dataframe" << endl;
    exit(-1);
  }

  /* fetch first num column from the DataFrame */
  int first_num_col;
  char col_type = '\0';
  for (int i = 0; i < df.ncols(); i++) {
    Schema &scm = df.get_schema();
    if (scm.col_type(i) == 'I' || scm.col_type(i) == 'F') {
      first_i_col = i;
      col_type = df.col_type(i);
    }
  }
  if (col_type == 'I') {
    cout << "Found int column, first 25 rows: \n| F |";
    for (int i = 0; i < 25; i++) {
      cout << "| ";
      if (i < df.nrows()) {
        cout << df.get_int(first_num_col, i);
      } else {
        cout << " ";
      }
      cout << " |" << endl;
    }
  } else if (col_type == 'F') {
    cout << "Found float column, first 25 rows: \n| F |";
    for (int i = 0; i < 25; i++) {
      cout << "| ";
      if (i < df.nrows()) {
        cout << df.get_float(first_num_col, i);
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
    int sum = 0;
    bool accept(Row &r) {
      for (int i = 0; i < r.width(); i++) {
        if (r.col_type(i) == 'I') {
          sum += r.get_int(i);
        }
      }
    }

    void join_delete(Rower *o) {
      SumIntRower *other = dynamic_cast<SumIntRower *>(o);
      assert(other);
      sum += other.sum;
      delete o;
    }
  };
  SumIntRower rower;
  df.map(rower);
  cout << "Sum of all ints in DF: " << rower.sum << endl;
}
