#ifndef SKIP_LIST_HPP
#define SKIP_LIST_HPP

#include "utils/atomic_markable_reference.hpp"
#include "utils/random.hpp"
#include "utils/serialize.hpp"
#include <atomic>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <thread>
#include <vector>

/**
 * @brief A skip list providing lock-free concurrency via `AtomicMarkableReference`s.
 *
 * This class provides logarithmic average time complexity for insert, remove, and search operations.
 *
 * @tparam TKey The type of the key.
 * @tparam TValue The type of the value.
 */
template <typename TKey, typename TValue>
class SkipList {
private:
    /**
     * @brief A skip list node providing lock-free concurrency via `AtomicMarkableReference`s.
     *
     */
    struct SkipListNode {
        /**
         * @brief Construct a new skip list node object.
         *
         * Sets all forward pointers to `nullptr`.
         *
         * @param key The key associated with the node.
         * @param value The value associated with the key.
         * @param height The `top_level` of the node.
         */
        SkipListNode(const TKey &key, const std::optional<TValue> &value, const int32_t height)
            : key(key), value(value), top_level(height) {
            initialiseForward(height, nullptr);
        }

        /**
         * @brief Construct a new skip list node object.
         *
         * Sets all forward pointers to `forward_value`.
         *
         * @param key The key associated with the node.
         * @param height The `top_level` of the node.
         * @param forward_value The value to set all forward pointers.
         */
        SkipListNode(const TKey &key, const int32_t height, SkipListNode *forward_value)
            : key(key), top_level(height) {
            initialiseForward(height, forward_value);
        }

        /**
         * @brief Destroy the skip list node object.
         *
         * Clears the underlying vector of forward pointers.
         */
        ~SkipListNode() {
            forward.clear();
        }

        /**
         * @brief Initialise the node's forward pointers to a given value.
         *
         * @param height The `top_level` of the node.
         * @param forward_value The value to set all forward pointers.
         */
        void initialiseForward(const int32_t height, SkipListNode *forward_value) {
            forward = std::vector<AtomicMarkableReference<SkipListNode>>(height);
            for (int32_t i = 0; i < height; ++i) {
                forward[i].set(forward_value, false);
            }
        }

        TKey key;                    // The key used to order and search the skip list.
        std::optional<TValue> value; // The associated value stored with the key.
        int32_t top_level;           // The highest level this node reaches in the skip list.

        std::vector<AtomicMarkableReference<SkipListNode>> forward; // The vector of `AtomicMarkableReference`s of forward pointers.
    };

    /**
     * @brief Search for a given key whilst performing garbage collection.
     *
     * Searches the skip list for a given key whilst cleaning up logically deleted
     * nodes. If the key is found, the method returns true, and the predecessors
     * and successors of the key are stored in the provided arrays.
     *
     * @param key The key to search for.
     * @param preds An array of pointers to the predecessors of the key at each level.
     * @param succs An array of pointers to successors of the key at each level.
     * @return `true` if the key is found.
     * @return `false` if the key is not found.
     */
    bool findWithGC(const TKey &key, SkipListNode **preds, SkipListNode **succs) {
        SkipListNode *pred, *curr, *succ;
        bool marked = false;
    RETRY:
        pred = head;
        for (auto level = MAX_LEVEL; level >= 0; --level) {
            curr = pred->forward[level].getReference();
            while (true) {
                succ = curr->forward[level].get(marked);
                while (marked) {
                    if (!pred->forward[level].compareAndSwap(curr, false, succ, false)) {
                        goto RETRY;
                    }
                    curr = pred->forward[level].getReference();
                    succ = curr->forward[level].get(marked);
                }
                if (curr->key >= key) {
                    break;
                }
                pred = curr;
                curr = succ;
            }
            preds[level] = pred;
            succs[level] = curr;
        }
        return (curr->key == key);
    };

    /**
     * @brief Generate the height of a newly inserted node via coin flips.
     *
     * The height (or level) of a node in the skip list is determined by a series
     * of coin flips, determining how many levels the node will occupy. Ensures
     * that the height does not exceed the max number of levels in a skip list.
     *
     * @return int32_t The random level generated.
     */
    int32_t randomLevel() const {
        int32_t new_level = 1;

        while (((double)rand() / (RAND_MAX)) < SKIP_LIST_PROBABILITY && new_level < MAX_LEVEL) {
            ++new_level;
        }
        return std::min(new_level, MAX_LEVEL);
    };

    /**
     * @brief Serialize a node of a skip list into a file.
     *
     * Serializes a skip list node, including its key, value, and forward
     * pointers into a binary format file for storage.
     *
     * @param ofs The `ofstream` the serialized node is read into.
     * @param node The node to serialize.
     */
    void serializeNode(std::ofstream &ofs, SkipListNode *node) const {
        while (node != nil) {
            serializeValue(ofs, node->key);
            serializeValue(ofs, node->value);
            ofs.write(reinterpret_cast<const char *>(&node->top_level), sizeof(node->top_level));

            for (int32_t i = 0; i < node->top_level; ++i) {
                bool marked;
                SkipListNode *next = node->forward[i].get(marked);
                bool is_nil = (next == nil);
                ofs.write(reinterpret_cast<const char *>(&is_nil), sizeof(is_nil));
                ofs.write(reinterpret_cast<const char *>(&marked), sizeof(marked));
            }

            node = node->forward[0].getReference();
        }

        int32_t sentinel = -1;
        ofs.write(reinterpret_cast<const char *>(&sentinel), sizeof(sentinel));
    }

    /**
     * @brief Deserialize a skip list file into the current skip list.
     *
     * Reads from a binary file to reconstruct the skip list. Re-inserts
     * nodes into the list whilst maintaining original structure, including
     * the key, value, and forward pointers.
     *
     * @param ifs The `ifstream` to deserialize from.
     */
    void deserializeNode(std::ifstream &ifs) {
        SkipListNode *prev = head;
        while (true) {
            TKey key;
            std::optional<TValue> value;
            int32_t top_level;

            deserializeValue(ifs, key);
            if (ifs.eof()) {
                break;
            }

            deserializeValue(ifs, value);
            ifs.read(reinterpret_cast<char *>(&top_level), sizeof(top_level));

            if (top_level == -1) {
                break;
            }

            SkipListNode *new_node = new SkipListNode(key, value, top_level);

            for (int32_t i = 0; i < top_level; ++i) {
                bool is_nil, marked;
                ifs.read(reinterpret_cast<char *>(&is_nil), sizeof(is_nil));
                ifs.read(reinterpret_cast<char *>(&marked), sizeof(marked));
                new_node->forward[i].set(is_nil ? nil : nullptr, marked);
            }

            for (int32_t i = 0; i < std::min(prev->top_level, new_node->top_level); ++i) {
                new_node->forward[i].set(prev->forward[i].getReference(), false);
                prev->forward[i].set(new_node, false);
            }

            prev = new_node;
        }
    }

    static constexpr int32_t MAX_LEVEL = 16;       // The max level of a skip list.
    static constexpr size_t RETRY_THRESHOLD = 100; // The number of failed compare-and-search operations before exponentiating backoff.
    const float SKIP_LIST_PROBABILITY = 0.5;       // The probability associated with random level generation.

    SkipListNode *head; // The head node of the skip list.
    SkipListNode *nil;  // The sentinel node used to mark the end of the list at all levels.

public:
    /**
     * @brief Construct a new skip list object.
     *
     * Initialises `nil` and `head` with a height of `MAX_LEVEL + 1`.
     */
    SkipList() {
        nil = new SkipListNode(std::numeric_limits<TKey>::max(), MAX_LEVEL + 1, nullptr);
        head = new SkipListNode(std::numeric_limits<TKey>::min(), MAX_LEVEL + 1, nil);
    };

    /**
     * @brief Destroy the skip list object.
     *
     * Deletes `nil` and `head`.
     */
    ~SkipList() {
        delete head;
        delete nil;
    };

    /**
     * @brief Search for a given key as a wait-free operation.
     *
     * @param key The key to search for.
     * @return `std::optional<TValue> *` A pointer to the associated optional value if found, otherwise `nullptr`.
     */
    std::optional<TValue> *findWaitFree(const TKey &key) const {
        bool marked = false;
        SkipListNode *pred = head, *curr = nullptr, *succ = nullptr;

        for (int32_t level = MAX_LEVEL; level >= 0; --level) {
            curr = pred->forward[level].getReference();
            while (true) {
                succ = curr->forward[level].get(marked);
                while (marked) {
                    curr = pred->forward[level].getReference();
                    succ = curr->forward[level].get(marked);
                }
                if (curr->key >= key) {
                    break;
                }
                pred = curr;
                curr = succ;
            }
        }
        return (curr->key == key ? &curr->value : nullptr);
    };

    /**
     * @brief Insert a given key-value pair into the skip list.
     *
     * If the key already exists, the value associated with the key is updated. Otherwise, the method
     * uses a probabilistic approach to determine the level (or height) of the new node.
     *
     * First, it finds the appropriate position for the new node, then creates a new node with the
     * determined height. Finally, it updates the forward pointers of the predecessors to point to
     * the new node.
     *
     * For each level, past a certain number of failed compare-and-swap operations, backoff will
     * exponentially increase to minimise future contention.
     *
     * @param key The key to insert.
     * @param value The associated value of the key.
     */
    void insert(const TKey &key, const std::optional<TValue> &value) {
        int32_t top_level = randomLevel();
        SkipListNode *preds[MAX_LEVEL + 1];
        SkipListNode *succs[MAX_LEVEL + 1];

        while (true) {
            if (findWithGC(key, preds, succs)) {
                SkipListNode *existing_node = succs[0];
                existing_node->value = value;
                return;
            }

            auto new_node = new SkipListNode(key, value, top_level);
            for (int32_t level = 0; level < top_level; ++level) {
                new_node->forward[level].set(succs[level], false);
            }

            auto pred = preds[0];
            auto succ = succs[0];
            new_node->forward[0].set(succ, false);
            if (!pred->forward[0].compareAndSwap(succ, false, new_node, false)) {
                continue;
            }

            for (int32_t level = 0; level < top_level; ++level) {
                int32_t backoff = 1, retries = 0;
                while (true) {
                    pred = preds[level];
                    succ = succs[level];

                    if (pred->forward[level].compareAndSwap(succ, false, new_node, false)) {
                        break;
                    }

                    std::this_thread::sleep_for(std::chrono::nanoseconds(backoff));
                    if (retries++ > RETRY_THRESHOLD) {
                        backoff = std::min(backoff * 2, 1000000);
                    }

                    findWithGC(key, preds, succs);
                }
            }
        }
    };

    /**
     * @brief Remove a key from the skip list.
     *
     * @param key The key to remove.
     * @return true The key was removed.
     * @return false The key was not removed.
     */
    bool remove(const TKey &key) {
        SkipListNode *preds[MAX_LEVEL + 1];
        SkipListNode *succs[MAX_LEVEL + 1];
        SkipListNode *succ;

        while (true) {
            bool found = findWithGC(key, preds, succs);
            if (!found) {
                return false;
            }

            bool marked = false;
            auto node_to_remove = succs[0];
            for (auto level = node_to_remove->top_level - 1; level >= 1; --level) {
                succ = node_to_remove->forward[level].get(marked);
                while (!marked) {
                    node_to_remove->forward[level].setMarked(true);
                    succ = node_to_remove->forward[level].get(marked);
                }
            }

            marked = false;
            succ = node_to_remove->forward[0].get(marked);
            while (true) {
                bool success = node_to_remove->forward[0].compareAndSwap(succ, false, succ, true);
                succ = succs[0]->forward[0].get(marked);
                if (success) {
                    return true;
                }
                if (marked) {
                    return false;
                }
            }
        }
    };

    /**
     * @brief Serialize the skip list.
     *
     * Serializes the entire skip list, including all its nodes, into a binary file.
     * Allows the structure and data to be persisted and restored in the future.
     *
     * @param filename The name of the file to serialize into.
     * @throws `std::runtime_error` if the file cannot be opened for writing.
     */
    void serialize(const std::string &filename) const {
        std::ofstream ofs(filename, std::ios::binary);
        if (!ofs) {
            throw std::runtime_error("unable to open file for writing skip list");
        }

        serializeNode(ofs, head->forward[0].getReference());
    }

    /**
     * @brief Deserialize the skip list.
     *
     * Loads a skip list from a binary file, restoring its structure and data.
     * Replaces any current skip list with the deserialized skip list.
     *
     * @param filename The name of the file to deserialize from.
     * @throws `std::runtime_error` if the file cannot be opened for reading.
     */
    void deserialize(const std::string &filename) {
        std::ifstream ifs(filename, std::ios::binary);
        if (!ifs) {
            throw std::runtime_error("unable to open file for reading skip list");
        }

        SkipListNode *current = head->forward[0].getReference();
        while (current != nil) {
            SkipListNode *next = current->forward[0].getReference();
            delete current;
            current = next;
        }
        for (int i = 0; i < MAX_LEVEL; ++i) {
            head->forward[i].set(nil, false);
        }

        deserializeNode(ifs);
    }
};

#endif
