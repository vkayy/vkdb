#ifndef ATOMIC_MARKABLE_REFERENCE_HPP
#define ATOMIC_MARKABLE_REFERENCE_HPP

#include <atomic>

/**
 * @brief A markable reference to a value.
 *
 * @tparam T The type of the value.
 */
template <typename T>
class MarkableReference {
public:
    /**
     * @brief Construct a new markable reference object from a `MarkableReference`.
     *
     * @param other The `MarkableReference` to construct from.
     */
    MarkableReference(MarkableReference &other)
        : value(other.value), marked(other.marked) {}

    /**
     * @brief Construct a new markable reference from a pointer and a mark.
     *
     * @param value The pointer to the value.
     * @param mark The mark.
     */
    MarkableReference(T *value, bool mark)
        : value(value), marked(mark) {}

    T *value = nullptr;
    bool marked = false;
};

/**
 * @brief An atomic markable reference to a value.
 *
 * @tparam T The type of the value.
 */
template <typename T>
class AtomicMarkableReference {
private:
    std::atomic<MarkableReference<T> *> ref; // The underlying markable reference reference.

public:
    /**
     * @brief Construct a new atomic markable reference object with default values.
     *
     * Initialises the markable reference's `value` to `nullptr` and `marked` to `false`.
     */
    AtomicMarkableReference() {
        ref.store(new MarkableReference<T>(nullptr, false));
    }

    /**
     * @brief Construct a new atomic markable reference object with the given values.
     *
     * @param value The value to set the reference's `value` to.
     * @param mark The mark to set the reference's `marked` to.
     */
    AtomicMarkableReference(T *value, bool mark) {
        ref.store(new MarkableReference<T>(value, mark));
    }

    /**
     * @brief Destroy the atomic markable reference object.
     *
     * Deletes the reference.
     */
    ~AtomicMarkableReference() {
        MarkableReference<T> *temp = ref.load();
        delete temp;
    }

    /**
     * @brief Get the reference's value.
     *
     * @return `T *` The reference's value.
     */
    T *getReference() {
        return ref.load()->value;
    }

    /**
     * @brief Get the reference's marked status and value.
     *
     * @param mark The variable to set to the reference's `marked`.
     * @return `T *` The reference's `value`.
     */
    T *get(bool &mark) {
        MarkableReference<T> *temp = ref.load();
        mark = temp->marked;
        return temp->value;
    }

    /**
     * @brief Sets the reference to a new value and marked status.
     *
     * If the current value or marked status differs from those provided, the reference
     * is updated to point to a new `MarkableReference` with the given value and mark.
     *
     * @param value The new value.
     * @param mark The new mark.
     */
    void set(T *value, bool mark) {
        MarkableReference<T> *curr = ref.load();
        if (value != curr->value || mark != curr->marked) {
            ref.store(new MarkableReference<T>(value, mark));
        }
    }

    /**
     * @brief Set the marked status of the reference.
     *
     * If the current marked status differs from the `mark` parameter, the reference
     * is updated to point to a new `MarkableReference` with the same value and new mark.
     *
     * @param mark The new marked status.
     */
    void setMarked(bool mark) {
        MarkableReference<T> *curr = ref.load();
        if (mark != curr->marked) {
            ref.store(new MarkableReference<T>(curr->value, mark));
        }
    }

    /**
     * @brief Perform an atomic compare-and-swap operation on the reference.
     *
     * If the current value and marked status match the expected values, the reference
     * is atomically updated to point to a new `MarkableReference` object with the specified
     * new value and marked status.
     *
     * @param expected_value Pointer to the expected value currently stored.
     * @param expected_mark Boolean flag indicating the expected marked status currently stored.
     * @param new_value Pointer to the new value to be stored if the operation succeeds.
     * @param new_mark Boolean flag indicating the new marked status to be stored if the operation succeeds.
     * @return `true` if the reference was successfully updated.
     * @return `false` if the reference did not match the expected values.
     */
    bool compareAndSwap(T *expected_value, bool expected_mark, T *new_value, bool new_mark) {
        MarkableReference<T> *curr = ref.load();
        return (expected_value == curr->value && expected_mark == curr->marked &&
                ref.compare_exchange_strong(curr, new MarkableReference<T>(new_value, new_mark)));
    }
};

#endif // ATOMIC_MARKABLE_REFERENCE_HPP
