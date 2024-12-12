#ifndef STORAGE_LSM_TREE_H
#define STORAGE_LSM_TREE_H

#include "utils/concepts.h"
#include "storage/sstable.h"
#include "storage/mem_table.h"
#include <ranges>

template <ArithmeticNoCVRefQuals TValue>
class LSMTree {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = std::optional<TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;

  explicit LSMTree(FilePath directory) noexcept
    : directory_{std::move(directory)}
    , sstable_id_{0} {}

  LSMTree(LSMTree&&) noexcept = default;
  LSMTree& operator=(LSMTree&&) noexcept = default;  

  LSMTree(const LSMTree&) = delete;
  LSMTree& operator=(const LSMTree&) = delete;

  ~LSMTree() {
    for (const auto& sstable : sstables_) {
      std::remove(sstable.filePath().c_str());
    }
  };

  void put(const key_type& key, const TValue& value) {
    mem_table_.put(key, value);
    if (mem_table_.size() == MemTable<TValue>::MAX_ENTRIES) {
      flush_memtable();
    }
  }

  [[nodiscard]] mapped_type get(const key_type& key) const {
    auto memtable_value{mem_table_.get(key)};
    if (memtable_value != std::nullopt) {
      return memtable_value;
    }
    for (const auto& sstable : sstables_ | std::views::reverse) {
      auto sstable_value{sstable.get(key)};
      if (sstable_value.has_value()) {
        return sstable_value;
      }
    }
    return std::nullopt;
  }

  [[nodiscard]] size_type sstableCount() const noexcept {
    return sstables_.size();
  }

private:
  using C0Layer = MemTable<TValue>;
  using C1Layer = std::vector<SSTable<TValue>>;

  void flush_memtable() {
    FilePath sstable_file_path{
      directory_ + "/sstable_" + std::to_string(sstable_id_++) + ".sst"
    };
    sstables_.emplace_back(sstable_file_path, std::move(mem_table_));
    mem_table_.clear();
  }

  C0Layer mem_table_;
  C1Layer sstables_;
  FilePath directory_;
  size_type sstable_id_;
};

#endif // STORAGE_LSM_TREE_H