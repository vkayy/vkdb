#ifndef STORAGE_LRU_CACHE_H
#define STORAGE_LRU_CACHE_H

#include <list>
#include <unordered_map>
#include <optional>
#include <utility>
#include <concepts>
#include <vkdb/concepts.h>

namespace vkdb {
/**
 * @brief A thread-safe LRU cache.
 * 
 * @tparam TKey Key type.
 * @tparam TValue Value type.
 */
template <RegularNoCVRefQuals TKey, RegularNoCVRefQuals TValue>
class LRUCache {
public:
  using key_type = TKey;
  using mapped_type = std::optional<TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;

  /**
   * @brief Construct a new LRUCache object.
   * 
   */
  LRUCache() noexcept
    : capacity_{DEFAULT_CAPACITY}
    , mutex_{std::make_unique<std::mutex>()} {}

  /**
   * @brief Construct a new LRUCache object given a capacity.
   * 
   * @param capacity The capacity.
   * 
   * @throws std::invalid_argument If the capacity is 0.
   */
  explicit LRUCache(size_type capacity)
    : capacity_{capacity}
    , mutex_{std::make_unique<std::mutex>()} {
      if (capacity == 0) {
        throw std::invalid_argument{
          "LRUCache(): Capacity must be greater than 0."
        };
      }
    }
  
  /**
   * @brief Move-construct a LRUCache object.
   * 
   */
  LRUCache(LRUCache&&) noexcept = default;

  /**
   * @brief Move-assign a LRUCache object.
   * 
   */
  LRUCache& operator=(LRUCache&&) noexcept = default;

  /**
   * @brief Deleted copy constructor.
   * 
   */
  LRUCache(const LRUCache&) = delete;

  /**
   * @brief Deleted copy assignment operator.
   * 
   */
  LRUCache& operator=(const LRUCache&) = delete;
  
  /**
   * @brief Destroy the LRUCache object.
   * 
   */
  ~LRUCache() noexcept = default;
  
  /**
   * @brief Put a key-value pair into the cache.
   * 
   * @tparam K Key type.
   * @tparam V Value-convertible type.
   * @param key Key.
   * @param value Value.
   * 
   * @throws std::exception If inserting the key-value pair fails.
   */
  template <SameNoCVRefQuals <key_type> K, std::convertible_to<mapped_type> V>
  void put(K&& key, V&& value) {
    std::lock_guard lock{*mutex_};
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

  /**
   * @brief Get a value from the cache.
   * 
   * @tparam K Key type.
   * @param key Key.
   * @return mapped_type The value if it exists, std::nullopt otherwise.
   */
  template <SameNoCVRefQuals <key_type> K>
  [[nodiscard]] mapped_type get(K&& key) {
    std::lock_guard lock{*mutex_};
    const auto it{map_.find(std::forward<K>(key))};
    if (it == map_.end()) {
      return std::nullopt;
    }
    list_.splice(list_.begin(), list_, it->second);
    return it->second->second;
  }

  /**
   * @brief Check if the cache contains a key.
   * 
   * @tparam K Key type.
   * @param key Key.
   * @return true if the cache contains the key.
   * @return false if the cache does not contain the key.
   */
  template <SameNoCVRefQuals <key_type> K>
  [[nodiscard]] bool contains(K&& key) const noexcept {
    std::lock_guard lock{*mutex_};
    return map_.find(std::forward<K>(key)) != map_.end();
  }

  /**
   * @brief Remove a key from the cache.
   * 
   * @tparam K Key type.
   * @param key Key.
   */
  [[nodiscard]] size_type capacity() const noexcept {
    std::lock_guard lock{*mutex_};
    return capacity_;
  }

  /**
   * @brief Get the size of the cache.
   * 
   * @return size_type The size of the cache.
   */
  [[nodiscard]] size_type size() const noexcept {
    std::lock_guard lock{*mutex_};
    return list_.size();
  }

  /**
   * @brief Clear the cache.
   * 
   */
  void clear() noexcept {
    std::lock_guard lock{*mutex_};
    list_.clear();
    map_.clear();
  }

private:
  /**
   * @brief Type alias for a list of key-value pairs.
   * 
   */
  using CacheList = std::list<value_type>;

  /**
   * @brief Type alias for a list iterator.
   * 
   */
  using CacheListIter = typename CacheList::iterator;

  /**
   * @brief Type alias for a map of keys to list iterators.
   * 
   */
  using CacheListIterMap = std::unordered_map<key_type, CacheListIter>;

  /**
   * @brief Default capacity.
   * 
   */
  static constexpr size_type DEFAULT_CAPACITY{1'000};

  /**
   * @brief Evict the least recently used key-value pair if needed.
   * 
   */
  void evict_if_needed() noexcept {
    if (list_.size() == capacity_) {
      map_.erase(list_.back().first);
      list_.pop_back();
    }
  }

  /**
   * @brief Capacity.
   * 
   */
  size_type capacity_;

  /**
   * @brief List of key-value pairs.
   * 
   */
  CacheList list_;

  /**
   * @brief Mapping from keys to list iterators.
   * 
   */
  CacheListIterMap map_;

  /**
   * @brief Mutex.
   * 
   */
  mutable std::unique_ptr<std::mutex> mutex_;
};
}  // namespace vkdb

#endif // STORAGE_LRU_CACHE_H