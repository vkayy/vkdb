#ifndef STORAGE_LRU_CACHE_H
#define STORAGE_LRU_CACHE_H

#include <list>
#include <unordered_map>
#include <optional>
#include <utility>
#include <concepts>
#include <vkdb/concepts.h>

namespace vkdb {
template <RegularNoCVRefQuals TKey, RegularNoCVRefQuals TValue>
class LRUCache {
public:
  using key_type = TKey;
  using mapped_type = TValue;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;
  using reference = value_type&;
  using const_reference = const value_type&;
  using mapped_ref_wrap = std::reference_wrapper<mapped_type>;
  using opt_mapped_ref_wrap = std::optional<mapped_ref_wrap>;

  LRUCache() noexcept
    : capacity_{DEFAULT_CAPACITY} {}

  explicit LRUCache(size_type capacity)
    : capacity_{capacity} {
      if (capacity == 0) {
        throw std::invalid_argument{
          "LRUCache(): Capacity must be greater than 0."
        };
      }
    }
  
  LRUCache(LRUCache&&) noexcept = default;
  LRUCache& operator=(LRUCache&&) noexcept = default;

  LRUCache(const LRUCache&) = delete;
  LRUCache& operator=(const LRUCache&) = delete;
  
  ~LRUCache() = default;
  
  template <SameNoCVRefQuals <key_type> K, SameNoCVRefQuals <mapped_type> V>
  void put(K&& key, V&& value) {
    std::lock_guard lock{mutex_};
    const auto it{map_.find(key)};
    if (it != map_.end()) {
      it->second->second = std::forward<V>(value);
      list_.splice(list_.begin(), list_, it->second);
      return;
    }

    evict_if_needed();
    list_.emplace_front(std::forward<K>(key), std::forward<V>(value));
    map_.try_emplace(list_.front().first, list_.begin());
  }

  template <SameNoCVRefQuals <key_type> K>
  [[nodiscard]] opt_mapped_ref_wrap get(K&& key) {
    std::lock_guard lock{mutex_};
    const auto it{map_.find(std::forward<K>(key))};
    if (it == map_.end()) {
      return std::nullopt;
    }
    list_.splice(list_.begin(), list_, it->second);
    return it->second->second;
  }

  template <SameNoCVRefQuals <key_type> K>
  [[nodiscard]] bool contains(K&& key) const noexcept {
    std::lock_guard lock{mutex_};
    return map_.find(std::forward<K>(key)) != map_.end();
  }

  [[nodiscard]] size_type capacity() const noexcept {
    std::lock_guard lock{mutex_};
    return capacity_;
  }

  [[nodiscard]] size_type size() const noexcept {
    std::lock_guard lock{mutex_};
    return list_.size();
  }

  void clear() noexcept {
    std::lock_guard lock{mutex_};
    list_.clear();
    map_.clear();
  }

private:
  using CacheList = std::list<value_type>;
  using CacheListIter = typename CacheList::iterator;
  using CacheListIterMap = std::unordered_map<key_type, CacheListIter>;

  static constexpr size_type DEFAULT_CAPACITY{1'000};

  void evict_if_needed() noexcept {
    if (list_.size() == capacity_) {
      map_.erase(list_.back().first);
      list_.pop_back();
    }
  }

  size_type capacity_;
  CacheList list_;
  CacheListIterMap map_;
  mutable std::mutex mutex_;
};
}  // namespace vkdb

#endif // STORAGE_LRU_CACHE_H