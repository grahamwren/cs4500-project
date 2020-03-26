#include "kd_store.h"
#include "key.h"
#include "dataframe.h"
#include "simple_dataframe.h"
#include "bytes_reader.h"
#include "bytes_writer.h"

DataFrame* KDStore::get(Key key) {
  BytesReader* reader = _kv.get(key);
  const char* s = (*reader->yield_string_ptr()).c_str();
  Schema scm(s);
  int rows = reader->yield<int>();

  DataFrame* df = new SimpleDataFrame(scm);
  for (int i = 0; i < rows; ++i) {
    Row* row = Row::unpack(*reader);
    df->add_row(*row);
    delete row;
  }
  
  delete reader;
  return df;
}

void KDStore::put(Key key, DataFrame& df) {
  BytesWriter writer(1000);
  Schema scm = df.get_schema();
  int len = df.nrows();
  Row row(scm);
  for (int i = 0; i < len; ++i) {
    df.fill_row(i, row);
    row.pack(writer);
  }
  _kv.put(key, writer);
}

