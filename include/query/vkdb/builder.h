#ifndef QUERY_BUILDER_H
#define QUERY_BUILDER_H

#include <vkdb/concepts.h>
#include <vkdb/lsm_tree.h>
#include <variant>
#include <ranges>
#include <ranges>
#include <algorithm>
#include <unordered_set>

namespace vkdb {
/**
 * @brief Type alias for a set of tag keys.
 * 
 */
using TagColumns = std::unordered_set<TagKey>;

/**
 * @brief Query builder for querying a Table.
 * 
 * @tparam TValue Type of the value of the TimeSeriesKey.
 */
template <ArithmeticNoCVRefQuals TValue>
class QueryBuilder {
public:
  using keytype = TimeSeriesKey;
  using mapped_type = std::optional<TValue>;
  using valuetype = std::pair<const keytype, mapped_type>;
  using size_type = uint64_t;
  using result_type = std::vector<valuetype>;

  /**
   * @brief Deleted default constructor.
   * 
   */
  QueryBuilder() = delete;

  /**
   * @brief Construct a new QueryBuilder object.
   * @details Query type is set to NONE and filters are initialised with a
   * filter that always returns true.
   * 
   * @param lsm_tree Reference to the LSMTree to query.
   * @param tag_columns Reference to the tag columns of the Table.
   */
  explicit QueryBuilder(
    LSMTree<TValue>& lsm_tree,
    const TagColumns& tag_columns
  ) noexcept
    : lsm_tree_{lsm_tree}
    , tag_columns_{tag_columns}
    , query_type_{QueryType::NONE}
    , filters_{TRUE_TIME_SERIES_KEY_FILTER} {}
  
  /**
   * @brief Move-construct a QueryBuilder object.
   * 
   */
  QueryBuilder(QueryBuilder&&) noexcept = default;

  /**
   * @brief Move-assign a QueryBuilder object.
   * 
   */
  QueryBuilder& operator=(QueryBuilder&&) noexcept = default;

  /**
   * @brief Copy-construct a QueryBuilder object.
   * 
   */
  QueryBuilder(const QueryBuilder&) noexcept = default;

  /**
   * @brief Copy-assign a QueryBuilder object.
   * 
   */
  QueryBuilder& operator=(const QueryBuilder&) noexcept = default;

  /**
   * @brief Destroy the Query Builder object.
   * 
   */
  ~QueryBuilder() noexcept = default;

  /**
   * @brief Configure builder for point query.
   * @details Query type is set to POINT and query parameters are set to the
   * TimeSeriesKey to query.
   * 
   * @param key TimeSeriesKey to query.
   * @return QueryBuilder& Reference to this QueryBuilder object.
   * 
   * @throw std::runtime_error If a tag in the key is not in the tag columns.
   */
  [[nodiscard]] QueryBuilder& point(const keytype& key) {
    validate_tags(key.tags());
    set_query_type(QueryType::POINT);
    query_params_ = QueryParams{PointParams{key}};
    return *this;
  }

  /**
   * @brief Configure builder for range query.
   * @details Query type is set to RANGE and query parameters are set to the
   * start and end TimeSeriesKeys of the range.
   * 
   * @param start Start TimeSeriesKey of the range.
   * @param end End TimeSeriesKey of the range.
   * @return QueryBuilder& Reference to this QueryBuilder object.
   * 
   * @throw std::runtime_error If a tag in the start or end key is not in the
   * tag columns.
   */
  [[nodiscard]] QueryBuilder& range(const keytype& start, const keytype& end) {
    validate_tags(start.tags());
    validate_tags(end.tags());
    set_query_type(QueryType::RANGE);
    query_params_ = QueryParams{RangeParams{start, end}};
    return *this;
  }

  /**
   * @brief Filter the TimeSeriesKeys by a tag.
   * @details Adds a filter that checks if the TimeSeriesKey has the specified
   * tag.
   * 
   * @param key Tag key to filter by.
   * @param value Tag value to filter by.
   * @return QueryBuilder& Reference to this QueryBuilder object.
   * 
   * @throw std::runtime_error If the tag key is not in the tag columns.
   */
  [[nodiscard]] QueryBuilder& filterByTag(
    const TagKey& key,
    const TagValue& value
  ) {
    validate_tags(Tag{key, value});
    add_filter([key, value](const auto& k) {
      return k.tags().contains(key) && k.tags().at(key) == value;
    });
    return *this;
  }

  /**
   * @brief Filter the TimeSeriesKeys by multiple tags.
   * @details Adds a filter that checks if the TimeSeriesKey has any of the
   * specified tags.
   * 
   * @tparam Tags Variadic template parameter for tags to filter by.
   * @param tags Tags to filter by.
   * @return QueryBuilder& Reference to this QueryBuilder object.
   * 
   * @throw std::runtime_error If a tag key is not in the tag columns.
   */
  template <AllConvertibleToNoCVRefQuals<Tag>... Tags>
  [[nodiscard]] QueryBuilder& filterByAnyTags(const Tags&... tags) {
    validate_tags(tags...);
    add_filter([tags...](const auto& k) {
      return ((k.tags().contains(tags.first) &&
               k.tags().at(tags.first) == tags.second) || ...);
    });
    return *this;
  }

  /**
   * @brief Filter the TimeSeriesKeys by multiple tags.
   * @details Adds a filter that checks if the TimeSeriesKey has all of the
   * specified tags.
   * 
   * @tparam Tags Variadic template parameter for tags to filter by.
   * @param tags Tags to filter by.
   * @return QueryBuilder& Reference to this QueryBuilder object.
   * 
   * @throw std::runtime_error If a tag key is not in the tag columns.
   */
  template <AllConvertibleToNoCVRefQuals<Tag>... Tags>
  [[nodiscard]] QueryBuilder& filterByAllTags(const Tags&... tags) {
    validate_tags(tags...);
    add_filter([tags...](const auto& k) {
      return ((k.tags().contains(tags.first) &&
               k.tags().at(tags.first) == tags.second) && ...);
    });
    return *this;
  }

  /**
   * @brief Filter the TimeSeriesKeys by a metric.
   * @details Adds a filter that checks if the TimeSeriesKey has the specified
   * metric.
   * 
   * @param metric Metric to filter by.
   * @return QueryBuilder& Reference to this QueryBuilder object.
   * 
   * @throw std::runtime_error If adding the filter fails.
   */
  [[nodiscard]] QueryBuilder& filterByMetric(const Metric& metric) {
    add_filter([metric](const auto& k) {
      return k.metric() == metric;
    });
    return *this;
  }

  /**
   * @brief Filter the TimeSeriesKeys by multiple metrics.
   * @details Adds a filter that checks if the TimeSeriesKey has any of the
   * specified metrics.
   * 
   * @tparam Metrics Variadic template parameter for metrics to filter by.
   * @param metrics Metrics to filter by.
   * @return QueryBuilder& Reference to this QueryBuilder object.
   * 
   * @throw std::runtime_error If adding the filter fails.
   */
  template <AllConvertibleToNoCVRefQuals<Metric>... Metrics>
  [[nodiscard]] QueryBuilder& filterByAnyMetrics(const Metrics&... metrics) {
    add_filter([metrics...](const auto& k) {
      return ((k.metric() == metrics) || ...);
    });
    return *this;
  }

  /**
   * @brief Filter the TimeSeriesKeys by a timestamp.
   * @details Adds a filter that checks if the TimeSeriesKey has the specified
   * timestamp.
   * 
   * @param timestamp Timestamp to filter by.
   * @return QueryBuilder& Reference to this QueryBuilder object.
   * 
   * @throw std::runtime_error If adding the filter fails.
   */
  [[nodiscard]] QueryBuilder& filterByTimestamp(const Timestamp& timestamp) {
    add_filter([timestamp](const auto& k) {
      return k.timestamp() == timestamp;
    });
    return *this;
  }

  /**
   * @brief Filter the TimeSeriesKeys by multiple timestamps.
   * @details Adds a filter that checks if the TimeSeriesKey has any of the
   * specified timestamps.
   * 
   * @tparam Timestamps Variadic template parameter for timestamps to filter by.
   * @param timestamps Timestamps to filter by.
   * @return QueryBuilder& Reference to this QueryBuilder object.
   * 
   * @throw std::runtime_error If adding the filter fails.
   */
  template <AllConvertibleToNoCVRefQuals<Timestamp>... Timestamps>
  [[nodiscard]] QueryBuilder& filterByAnyTimestamps(
    const Timestamps&... timestamps
  ) {
    add_filter([timestamps...](const auto& k) {
      return ((k.timestamp() == timestamps) || ...);
    });
    return *this;
  }

  /**
   * @brief Put a key-value pair into the LSMTree.
   * 
   * @param key Key to put.
   * @param value Value to put.
   * @return QueryBuilder& Reference to this QueryBuilder object.
   * 
   * @throw std::runtime_error If any of the key's tags are not in the tag
   * columns.
   */
  [[nodiscard]] QueryBuilder& put(const keytype& key, const TValue& value) {
    validate_tags(key.tags());
    set_query_type(QueryType::PUT);
    query_params_ = QueryParams{PutParams{key, value}};
    return *this;
  }

  /**
   * @brief Remove a key from the LSMTree.
   * 
   * @param key Key to remove.
   * @return QueryBuilder& Reference to this QueryBuilder object.
   * 
   * @throw std::runtime_error If any of the key's tags are not in the tag
   * columns.
   */
  [[nodiscard]] QueryBuilder& remove(const keytype& key) {
    validate_tags(key.tags());
    set_query_type(QueryType::REMOVE);
    query_params_ = QueryParams{RemoveParams{key}};
    return *this;
  }

  /**
   * @brief Count the number of entries in the range.
   * @details Sets up the QueryBuilder for aggregation and returns the number
   * of entries in the range.
   * 
   * @return size_type Number of entries in the range.
   * 
   * @throw std::runtime_error If the aggregate setup fails.
   */
  [[nodiscard]] size_type count() {
    setup_aggregate();
    return std::ranges::distance(get_filtered_range());
  }

  /**
   * @brief Sum the values in the range.
   * @details Sets up the QueryBuilder for aggregation and returns the sum of
   * the values in the range.
   * 
   * @return TValue Sum of the values in the range.
   * 
   * @throw std::runtime_error If the aggregate setup fails or the range is
   * empty.
   */
  [[nodiscard]] TValue sum() {
    setup_aggregate();
    auto range{get_nonempty_filtered_range()};
    return std::accumulate(range.begin(), range.end(), TValue{},
      [](const TValue& acc, const valuetype& entry) {
        return acc + entry.second.value();
      });
  }

  /**
   * @brief Calculate the average of the values in the range.
   * @details Sets up the QueryBuilder for aggregation and returns the average
   * of the values in the range.
   * 
   * @return double Average of the values in the range.
   * 
   * @throw std::runtime_error If the aggregate setup fails or the range is
   * empty.
   */
  [[nodiscard]] double avg() {
    setup_aggregate();
    auto range{get_nonempty_filtered_range()};
    auto sum{std::accumulate(range.begin(), range.end(), TValue{},
      [](const auto& acc, const auto& entry) {
        return acc + entry.second.value();
      })};
    return static_cast<double>(sum) / std::ranges::distance(range);
  }

  /**
   * @brief Calculate the minimum value in the range.
   * @details Sets up the QueryBuilder for aggregation and returns the minimum
   * value in the range.
   * 
   * @return TValue Minimum value in the range.
   * 
   * @throw std::runtime_error If the aggregate setup fails or the range is
   * empty.
   */
  [[nodiscard]] TValue min() {
    setup_aggregate();
    auto range{get_nonempty_filtered_range()};
    return std::ranges::min_element(range, {}, [](const auto& entry) {
      return entry.second.value();
    })->second.value();
  }

  /**
   * @brief Calculate the maximum value in the range.
   * @details Sets up the QueryBuilder for aggregation and returns the maximum
   * value in the range.
   * 
   * @return TValue Maximum value in the range.
   * 
   * @throw std::runtime_error If the aggregate setup fails or the range is
   * empty.
   */
  [[nodiscard]] TValue max() {
    setup_aggregate();
    auto range{get_nonempty_filtered_range()};
    return std::ranges::max_element(range, {}, [](const auto& entry) {
      return entry.second.value();
    })->second.value();
  }

  /**
   * @brief Execute the query.
   * @details Executes the query based on the query type and query parameters.
   * 
   * @return result_type Result of the query.
   * 
   * @throw std::runtime_error If the query type is not set and there are no
   * filters or if executing the query fails.
   */
  [[nodiscard]]
  result_type execute() {
    switch (query_type_) {
    case QueryType::NONE:
      if (filters_.size() == 1) {
        throw std::runtime_error{
          "QueryBuilder::execute(): No query type specified."
        };
      }
      set_default_range_if_none();
      return get_filtered_range();
    case QueryType::POINT:
      return execute_point_query();
    case QueryType::RANGE:
      return execute_range_query();
    case QueryType::PUT:
      return execute_put_query();
    case QueryType::REMOVE:
      return execute_remove_query();
    }
  }

private:
  /**
   * @brief Type of query.
   * 
   */
  enum class QueryType { NONE, POINT, RANGE, PUT, REMOVE };

  /**
   * @brief Parameters for a point query.
   * 
   */
  struct PointParams {
    /**
     * @brief Key to query.
     * 
     */
    keytype key;
  };

  /**
   * @brief Parameters for a range query.
   * 
   */
  struct RangeParams {
    /**
     * @brief Start of the range.
     * 
     */
    keytype start;

    /**
     * @brief End of the range.
     * 
     */
    keytype end;
  };

  /**
   * @brief Parameters for a put query.
   * 
   */
  struct PutParams {
    /**
     * @brief Key to put.
     * 
     */
    keytype key;

    /**
     * @brief Value to put.
     * 
     */
    TValue value;
  };

  /**
   * @brief Parameters for a remove query.
   * 
   */
  struct RemoveParams {
    /**
     * @brief Key to remove.
     * 
     */
    keytype key;
  };

  /**
   * @brief Query parameters.
   * @details Variant of point, range, put, and remove parameters.
   * 
   */
  using QueryParams = std::variant<
    std::monostate,
    PointParams,
    RangeParams,
    PutParams,
    RemoveParams
  >;

  /**
   * @brief Set the query type.
   * 
   * @param query_type Query type to set.
   * 
   * @throws std::runtime_error If the query type is already set.
   */
  void set_query_type(QueryType query_type) {
    if (query_type_ != QueryType::NONE) {
      throw std::runtime_error{
        "QueryBuilder::set_query_type(): Query type already set."
      };
    }
    query_type_ = query_type;
  }

  /**
   * @brief Sets default range if the query type is NONE.
   * @details Sets the range to the entire range of the LSMTree if the query
   * type is NONE.
   * 
   */
  void set_default_range_if_none() noexcept {
    if (query_type_ == QueryType::NONE) {
      query_params_ = QueryParams{RangeParams{
        MIN_TIME_SERIES_KEY,
        MAX_TIME_SERIES_KEY
      }};
      query_type_ = QueryType::RANGE;
    }
  }

  /**
   * @brief Check if the query type is aggregable.
   * @details Query type is aggregable if it is RANGE or POINT.
   * 
   * @throw std::runtime_error If the query type is not aggregable.
   * 
   */
  void check_if_aggregable() const {
    if (query_type_ != QueryType::RANGE && query_type_ != QueryType::POINT) {
      throw std::runtime_error{
        "QueryBuilder::check_if_aggregable(): Query type must be aggregable."
      };
    }
  }

  /**
   * @brief Set up the QueryBuilder for aggregation.
   * @details Sets the default range if the query type is NONE and checks if
   * the query type is aggregable.
   * 
   * @throw std::runtime_error If the query type is not aggregable.
   */
  void setup_aggregate() {
    set_default_range_if_none();
    check_if_aggregable();
  }

  /**
   * @brief Add a filter to the filters vector.
   * 
   * @param filter TimeSeriesKeyFilter to add.
   */
  void add_filter(TimeSeriesKeyFilter&& filter) {
    filters_.push_back(std::move(filter));
  }

  /**
   * @brief Get the filtered range.
   * @details Filters the range based on the filters and returns it.
   * 
   * @return result_type Filtered range.
   */
  [[nodiscard]] result_type get_filtered_range() const {
    if (query_type_ == QueryType::POINT) {
      return execute_point_query();
    }
    const auto& params{std::get<RangeParams>(query_params_)};
    return lsm_tree_.getRange(params.start, params.end,
      [this](const auto& k) {
        return std::ranges::all_of(filters_, [&k](const auto& filter) {
          return filter(k);
        });
      });
  }

  /**
   * @brief Get the non-empty filtered range.
   * @details Gets the filtered range and ensures it is non-empty.
   * 
   * @return result_type Non-empty filtered range.
   * 
   * @throw std::runtime_error If the filtered range is empty.
   */
  [[nodiscard]] result_type get_nonempty_filtered_range() const {
    auto filtered_range{get_filtered_range()};
    if (filtered_range.empty()) {
      throw std::runtime_error{
        "QueryBuilder::get_nonempty_filtered_range(): "
        "Cannot aggregate on empty range."
      };
    }
    return filtered_range;
  }

  /**
   * @brief Execute a point query.
   * 
   * @return result_type Result of the point query.
   * 
   * @throw std::runtime_error If getting the value fails.
   */
  [[nodiscard]] result_type execute_point_query() const {
    const auto& params{std::get<PointParams>(query_params_)};
    auto value{lsm_tree_.get(params.key)};
    if (!value.has_value()) {
      return {};
    }
    return {{params.key, value}};
  }

  /**
   * @brief Execute a range query.
   * 
   * @return result_type Result of the range query.
   * 
   * @throw std::runtime_error If getting the range fails.
   */
  [[nodiscard]] result_type execute_range_query() const {
    auto range{get_filtered_range()};
    return {range.begin(), range.end()};
  }

  /**
   * @brief Execute a put query.
   * 
   * @return result_type Result of the put query.
   * 
   * @throw std::runtime_error If putting the key-value pair fails.
   */
  [[nodiscard]] result_type execute_put_query() {
    const auto& params{std::get<PutParams>(query_params_)};
    lsm_tree_.put(params.key, params.value);
    return {};
  }

  /**
   * @brief Execute a remove query.
   * 
   * @return result_type Result of the remove query.
   * 
   * @throw std::runtime_error If removing the key fails.
   */
  [[nodiscard]] result_type execute_remove_query() {
    const auto& params{std::get<RemoveParams>(query_params_)};
    lsm_tree_.remove(params.key);
    return {};
  }

  /**
   * @brief Validate tags.
   * @details Checks if all the tag keys are in the tag columns.
   * 
   * @param tag_table TagTable to validate.
   * 
   * @throw std::runtime_error If a tag key is not in the tag columns.
   */
  void validate_tags(const TagTable& tag_table) const {
    for (const auto& [key, value] : tag_table) {
      if (!tag_columns_.contains(key)) {
        throw std::runtime_error{
          "QueryBuilder::validate_tags(): Tag '"
          + key + "' not in tag columns."
        };
      }
    }
  }

  /**
   * @brief Validate tags.
   * @details Checks if all the tag keys are in the tag columns.
   * 
   * @tparam Tags Variadic template parameter for tags.
   * @param tags Tags to validate.
   * 
   * @throw std::runtime_error If a tag key is not in the tag columns.
   */
  template <AllSameNoCVRefQuals<Tag>... Tags>
  void validate_tags(const Tags&... tags) const {
    for (const auto& [key, value] : std::initializer_list<Tag>{tags...}) {
      if (!tag_columns_.contains(key)) {
        throw std::runtime_error{
          "QueryBuilder::validate_tags(): Tag '"
          + key + "' not in tag columns."
        };
      }
    }
  }

  /**
   * @brief Reference to the LSMTree to query.
   * 
   */
  LSMTree<TValue>& lsm_tree_;

  /**
   * @brief Tag columns of the Table.
   * 
   */
  const TagColumns& tag_columns_;

  /**
   * @brief Query type.
   * 
   */
  QueryType query_type_;

  /**
   * @brief Query parameters.
   * 
   */
  QueryParams query_params_;

  /**
   * @brief Filters for the TimeSeriesKeys.
   * 
   */
  std::vector<TimeSeriesKeyFilter> filters_;
};
}  // namespace vkdb

#endif // QUERY_BUILDER_H