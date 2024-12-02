#ifndef STORAGE_LRU_CACHE_H
#define STORAGE_LRU_CACHE_H

#include <list>
#include <unordered_map>
#include <optional>
#include <utility>
#include <concepts>
#include "utils/concepts.h"

using CacheCapacity = size_t;

template <std::regular TKey, std::regular TValue>
  requires std::is_same_v<TKey, std::remove_cvref_t<TKey>> &&
            std::is_same_v<TValue, std::remove_cvref_t<TValue>>
class LRUCache {
public:
  using key_type = TKey;
  using mapped_type = TValue;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = CacheCapacity;
  using reference = value_type&;
  using const_reference = const value_type&;
  using const_mapped_ref_wrap = std::reference_wrapper<const mapped_type>;
  using opt_const_mapped_ref_wrap = std::optional<const_mapped_ref_wrap>;

  explicit LRUCache(CacheCapacity capacity = DEFAULT_CAPACITY) noexcept(false)
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
  
  template <SameNoCVRefQuals <key_type> K, SameNoCVRefQuals <mapped_type> V>
  void put(K&& key, V&& value) {
    auto it{map_.find(key)};
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
  [[nodiscard]] opt_const_mapped_ref_wrap get(K&& key) {
    auto it{map_.find(std::forward<K>(key))};
    if (it == map_.end()) {
      return std::nullopt;
    }
    list_.splice(list_.begin(), list_, it->second);
    return it->second->second;
  }

  template <SameNoCVRefQuals <key_type> K>
  [[nodiscard]] bool contains(K&& key) const noexcept {
    return map_.find(std::forward<K>(key)) != map_.end();
  }

  [[nodiscard]] CacheCapacity capacity() const noexcept {
    return capacity_;
  }

  [[nodiscard]] CacheCapacity size() const noexcept {
    return list_.size();
  }

  void clear() noexcept {
    list_.clear();
    map_.clear();
  }

private:
  using CacheList = std::list<value_type>;
  using CacheListIter = typename CacheList::iterator;
  using CacheListIterMap = std::unordered_map<key_type, CacheListIter>;

  static constexpr CacheCapacity DEFAULT_CAPACITY{1'000};

  void evict_if_needed() noexcept {
    if (list_.size() == capacity_) {
      map_.erase(std::move(list_.back().first));
      list_.pop_back();
    }
  }

  CacheCapacity capacity_;
  CacheList list_;
  CacheListIterMap map_;
};

#endif // STORAGE_LRU_CACHE_H