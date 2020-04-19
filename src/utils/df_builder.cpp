#include "lib/data.h"
#include "lib/schema.h"
#include "lib/simple_dataframe.h"
#include <cassert>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <string>

using namespace std;

bool rand_bool() { return rand() % 2 == 0; }
int rand_int() {
  if (rand_bool())
    return rand();
  else
    return rand() * -1;
}
float rand_float() {
  int divis = 0;
  while (divis == 0) {
    divis = rand(); // always positive
  }
  return rand_int() / (float)divis;
}
string *rand_string(string *dest) {
  int len = rand() % 255; // allow length up to 255
  char c;
  for (int i = 0; i < len; i++) {
    c = (char)((rand() % 95) + 32); // ascii chars between 32 and 126
    if (c == '"')                   // don't allow double quotes in strings
      c = '\'';
    dest->append(1, c);
  }
  return dest;
}

/**
 * Script for generating dataframes in SOR format for testing parser. Takes 2
 * positional arguments: first the number of lines and second the schema of the
 * DataFrame.
 *
 * e.g.
 * $ df_builder 100000 ISFBSSIIIIIIISSBF
 */
int main(int argc, char **argv) {
  // use current time as seed for random generator
  srand(time(nullptr));

  assert(argc == 3);
  int length = atoi(argv[1]);
  Schema scm(argv[2]);
  SimpleDataFrame df(scm);

  Row r(scm);
  for (int y = 0; y < length; y++) {
    for (int x = 0; x < scm.width(); x++) {
      /* 1 in 5 chance for missing */
      if (rand() % 5 == 0) {
        r.set_missing(x);
      } else {
        switch (scm.col_type(x)) {
        case Data::Type::INT:
          r.set(x, rand_int());
          break;
        case Data::Type::FLOAT:
          r.set(x, rand_float());
          break;
        case Data::Type::BOOL:
          r.set(x, rand_bool());
          break;
        case Data::Type::STRING:
          r.set(x, rand_string(new string()));
          break;
        case Data::Type::MISSING:
          r.set_missing(x);
          break;
        }
      }
    }
    df.add_row(r);
  }
  df.print();
}
