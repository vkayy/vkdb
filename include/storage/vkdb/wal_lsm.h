#ifndef STORAGE_WAL_LSM_H
#define STORAGE_WAL_LSM_H

#include <vkdb/lsm_tree.h>

namespace vkdb {
template <ArithmeticNoCVRefQuals TValue>
class LSMTree;

template <ArithmeticNoCVRefQuals TValue>
class WriteAheadLog;

enum class WALRecordType {
  PUT,
  REMOVE
};

template <ArithmeticNoCVRefQuals TValue>
struct WALRecord {
  WALRecordType type;
  TimeSeriesEntry<TValue> entry;
};
}

#endif // STORAGE_WAL_LSM_H