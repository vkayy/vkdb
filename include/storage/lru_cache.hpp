#ifndef LRU_CACHE_HPP
#define LRU_CACHE_HPP

#include <iostream>
#include <list>
#include <map>

/**
 * @brief A simple LRU cache implementation.
 *
 * @tparam TKey The key type.
 * @tparam TValue The value type.
 */
template <typename TKey, typename TValue>
class LRUCache {
private:
    constexpr static size_t DEFAULT_CAPACITY = 100 * 1000 * 8; // The default capacity of the cache (100KB).

    size_t capacity;                                                                 // The capacity of the cache.
    std::list<std::pair<TKey, TValue>> cache_list;                                   // The list of key-value pairs.
    std::map<TKey, typename std::list<std::pair<TKey, TValue>>::iterator> cache_map; // The map of keys to iterators.

public:
    /**
     * @brief Construct a new LRUCache object.
     *
     * @param capacity The capacity of the cache.
     */
    LRUCache(size_t capacity = DEFAULT_CAPACITY)
        : capacity(capacity) {}

    /**
     * @brief Get the value associated with the key.
     *
     * @param key The key.
     * @return `TValue *` The value associated with the key.
     */
    TValue *get(const TKey &key) {
        auto it = cache_map.find(key);
        if (it == cache_map.end()) {
            return nullptr;
        }
        cache_list.splice(cache_list.begin(), cache_list, it->second);
        return &it->second->second;
    }

    /**
     * @brief Put the key-value pair into the cache.
     *
     * @param key The key.
     * @param value The value.
     */
    void put(const TKey &key, const TValue &value) {
        auto it = cache_map.find(key);
        if (it != cache_map.end()) {
            it->second->second = value;
            cache_list.splice(cache_list.begin(), cache_list, it->second);
        } else {
            if (cache_list.size() >= capacity) {
                auto lru = cache_list.back();
                cache_map.erase(lru.first);
                cache_list.pop_back();
            }
            cache_list.emplace_front(key, value);
            cache_map[key] = cache_list.begin();
        }
    }
};

#endif // LRU_CACHE_HPP
