#ifndef QUERY_FRIENDLY_BUILDER_H
#define QUERY_FRIENDLY_BUILDER_H

#include <vkdb/builder.h>

namespace vkdb {
/**
 * @brief Friendly query builder for querying a Table.
 * 
 * @tparam TValue Type of the value of the TimeSeriesKey.
 */
template <ArithmeticNoCVRefQuals TValue>
class FriendlyQueryBuilder {
public:
  using size_type = typename QueryBuilder<TValue>::size_type;
  using value_type = DataPoint<TValue>;
  using result_type = std::vector<value_type>;

  /**
   * @brief Deleted default constructor.
   * 
   */
  FriendlyQueryBuilder() = delete;

  /**
   * @brief Construct a new FriendlyQueryBuilder object.
   * @details Initialises the query builder with a QueryBuilder.
   * 
   * @param lsm_tree Reference to the LSMTree to query.
   * @param tag_columns Reference to the tag columns of the Table.
   */
  explicit FriendlyQueryBuilder(
    LSMTree<TValue>& lsm_tree,
    const TagColumns& tag_columns
  ) noexcept
   : query_builder_{QueryBuilder<TValue>(lsm_tree, tag_columns)} {}

  /**
   * @brief Construct a new FriendlyQueryBuilder object.
   * 
   * @param query_builder QueryBuilder to use.
   */
  explicit FriendlyQueryBuilder(QueryBuilder<TValue>&& query_builder)
    : query_builder_{std::move(query_builder)} {}

  /**
   * @brief Move-construct a FriendlyQueryBuilder object.
   * 
   */
  FriendlyQueryBuilder(FriendlyQueryBuilder&&) noexcept = default;

  /**
   * @brief Move-assign a FriendlyQueryBuilder object.
   * 
   */
  FriendlyQueryBuilder& operator=(
    FriendlyQueryBuilder&&
  ) noexcept = default;

  /**
   * @brief Copy-construct a FriendlyQueryBuilder object.
   * 
   */
  FriendlyQueryBuilder(const FriendlyQueryBuilder&) noexcept = default;
  
  /**
   * @brief Copy-assign a FriendlyQueryBuilder object.
   * 
   */
  FriendlyQueryBuilder& operator=(
    const FriendlyQueryBuilder&
  ) noexcept = default;

  /**
   * @brief Destroy the FriendlyQueryBuilder object.
   * 
   */
  ~FriendlyQueryBuilder() noexcept = default;

  /**
   * @brief Configure builder for get query.
   * @details Adds a point query to the query builder.
   * 
   * @param timestamp Timestamp of the TimeSeriesKey.
   * @param metric Metric of the TimeSeriesKey.
   * @param tag_table TagTable of the TimeSeriesKey.
   * @return FriendlyQueryBuilder& Reference to this FriendlyQueryBuilder
   * object.
   * 
   * @throw std::runtime_error If the point query fails.
   */
  [[nodiscard]] FriendlyQueryBuilder& get(
    Timestamp timestamp, Metric metric,
    const TagTable& tag_table
  ) {
    TimeSeriesKey key{timestamp, metric, tag_table};
    std::ignore = query_builder_.point(key);
    return *this;
  }

  /**
   * @brief Configure builder for where metric is clause.
   * 
   * @param metric Metric to filter by.
   * @return FriendlyQueryBuilder& Reference to this FriendlyQueryBuilder
   * object.
   * 
   * @throw std::runtime_error If adding the filter fails.
   */
  [[nodiscard]] FriendlyQueryBuilder& whereMetricIs(const Metric& metric) {
    std::ignore = query_builder_.filterByMetric(metric);
    return *this;
  }

  /**
   * @brief Configure builder for where metric is any of clause.
   * 
   * @tparam Metrics Variadic template parameter for metrics to filter by.
   * @param metrics Metrics to filter by.
   * @return FriendlyQueryBuilder& Reference to this FriendlyQueryBuilder
   * object.
   * 
   * @throw std::runtime_error If adding any of the filters fails.
   */
  template <AllConvertibleToNoCVRefQuals<Metric>... Metrics>
  [[nodiscard]] FriendlyQueryBuilder& whereMetricIsAnyOf(
    const Metrics&... metrics
  ) {
    std::ignore = query_builder_.filterByAnyMetrics(metrics...);
    return *this;
  }

  /**
   * @brief Configure builder for where timestamp is clause.
   * 
   * @param timestamp Timestamp to filter by.
   * @return FriendlyQueryBuilder& Reference to this FriendlyQueryBuilder
   * object.
   * 
   * @throw std::runtime_error If adding the filter fails.
   */
  [[nodiscard]] FriendlyQueryBuilder& whereTimestampIs(
    const Timestamp& timestamp
  ) {
    std::ignore = query_builder_.filterByTimestamp(timestamp);
    return *this;
  }

  /**
   * @brief Configure builder for where timestamp is between clause.
   * 
   * @param start Start timestamp.
   * @param end End timestamp.
   * @return FriendlyQueryBuilder& Reference to this FriendlyQueryBuilder
   * object.
   * 
   * @throw std::runtime_error If the range query fails.
   */
  [[nodiscard]] FriendlyQueryBuilder& whereTimestampBetween(
    const Timestamp& start,
    const Timestamp& end
  ) {
    TimeSeriesKey start_key{start, MIN_METRIC, {}};
    TimeSeriesKey end_key{end, MAX_METRIC, {}};
    std::ignore = query_builder_.range(start_key, end_key);
    return *this;
  }

  /**
   * @brief Configure builder for where timestamp is any of clause.
   * 
   * @tparam Timestamps Variadic template parameter for timestamps to filter by.
   * @param timestamps Timestamps to filter by.
   * @return FriendlyQueryBuilder& Reference to this FriendlyQueryBuilder
   * object.
   * 
   * @throw std::runtime_error If adding the filter fails.
   */
  template <AllConvertibleToNoCVRefQuals<Timestamp>... Timestamps>
  [[nodiscard]] FriendlyQueryBuilder& whereTimestampIsAnyOf(
    const Timestamps&... timestamps
  ) {
    std::ignore = query_builder_.filterByAnyTimestamps(timestamps...);
    return *this;
  }

  /**
   * @brief Configure builder for where tags contain clause.
   * 
   * @param tag Tag to filter by.
   * @return FriendlyQueryBuilder& Reference to this FriendlyQueryBuilder
   * object.
   * 
   * @throw std::runtime_error If adding the filter fails.
   */
  [[nodiscard]] FriendlyQueryBuilder& whereTagsContain(const Tag& tag) {
    std::ignore = query_builder_.filterByTag(tag.first, tag.second);
    return *this;
  }

  /**
   * @brief Configure builder for where tags contain any of clause.
   * 
   * @tparam Tags Variadic template parameter for tags to filter by.
   * @param tags Tags to filter by.
   * @return FriendlyQueryBuilder& Reference to this FriendlyQueryBuilder
   * object.
   * 
   * @throw std::runtime_error If adding any of the filters fails.
   */
  template <AllConvertibleToNoCVRefQuals<Tag>... Tags>
  [[nodiscard]] FriendlyQueryBuilder& whereTagsContainAnyOf(
    const Tags&... tags
  ) {
    std::ignore = query_builder_.filterByAnyTags(tags...);
    return *this;
  }

  /**
   * @brief Configure builder for where tags contain all of clause.
   * 
   * @tparam Tags Variadic template parameter for tags to filter by.
   * @param tags Tags to filter by.
   * @return FriendlyQueryBuilder& Reference to this FriendlyQueryBuilder
   * object.
   * 
   * @throw std::runtime_error If adding all of the filters fails.
   */
  template <AllConvertibleToNoCVRefQuals<Tag>... Tags>
  [[nodiscard]] FriendlyQueryBuilder& whereTagsContainAllOf(
    const Tags&... tags
  ) {
    std::ignore = query_builder_.filterByAllTags(tags...);
    return *this;
  }

  /**
   * @brief Configure builder for put query.
   * @details Adds a put query to the query builder.
   * 
   * @param timestamp Timestamp of the TimeSeriesKey.
   * @param metric Metric of the TimeSeriesKey.
   * @param tag_table TagTable of the TimeSeriesKey.
   * @param value Value of the TimeSeriesKey.
   * @return FriendlyQueryBuilder& Reference to this FriendlyQueryBuilder
   * object.
   * 
   * @throw std::runtime_error If the metric is invalid or the put query fails.
   */
  [[nodiscard]] FriendlyQueryBuilder& put(
    Timestamp timestamp,
    Metric metric,
    const TagTable& tag_table, TValue value
  ) {
    if (
      metric.empty() ||
      metric.length() >= TimeSeriesKey::MAX_METRIC_LENGTH
    ) {
      throw std::runtime_error{
        "FriendlyQueryBuilder::get(): Invalid metric '" + metric + "'."
      };
    }
    TimeSeriesKey key{timestamp, metric, tag_table};
    std::ignore = query_builder_.put(key, value);
    return *this;
  }

  /**
   * @brief Configure builder for remove query.
   * @details Adds a remove query to the query builder.
   * 
   * @param timestamp Timestamp of the TimeSeriesKey.
   * @param metric Metric of the TimeSeriesKey.
   * @param tag_table TagTable of the TimeSeriesKey.
   * @return FriendlyQueryBuilder& Reference to this FriendlyQueryBuilder
   * object.
   * 
   * @throw std::runtime_error If the remove query fails.
   */
  [[nodiscard]] FriendlyQueryBuilder& remove(
    Timestamp timestamp,
    Metric metric,
    const TagTable& tag_table
  ) {
    TimeSeriesKey key{timestamp, metric, tag_table};
    std::ignore = query_builder_.remove(key);
    return *this;
  }

  /**
   * @brief Count the number of entries in the range.
   * @details Sets up the QueryBuilder for aggregation and returns the number
   * of entries in the range.
   * 
   * @return size_type Number of entries in the range.
   * 
   * @throw std::runtime_error If the count query fails.
   */
  [[nodiscard]] size_type count() {
    return query_builder_.count();
  }

  /**
   * @brief Sum the values in the range.
   * @details Sets up the QueryBuilder for aggregation and returns the sum of
   * the values in the range.
   * 
   * @return TValue Sum of the values in the range.
   * 
   * @throw std::runtime_error If the sum query fails.
   */
  [[nodiscard]] TValue sum() {
    return query_builder_.sum();
  }

  /**
   * @brief Calculate the average of the values in the range.
   * @details Sets up the QueryBuilder for aggregation and returns the average
   * of the values in the range.
   * 
   * @return double Average of the values in the range.
   * 
   * @throw std::runtime_error If the average query fails.
   */
  [[nodiscard]] double avg() {
    return query_builder_.avg();
  }

  /**
   * @brief Calculate the minimum value in the range.
   * @details Sets up the QueryBuilder for aggregation and returns the minimum
   * value in the range.
   * 
   * @return TValue Minimum value in the range.
   * 
   * @throw std::runtime_error If the minimum query fails.
   */
  [[nodiscard]] TValue min() {
    return query_builder_.min();
  }
  
  /**
   * @brief Calculate the maximum value in the range.
   * @details Sets up the QueryBuilder for aggregation and returns the maximum
   * value in the range.
   * 
   * @return TValue Maximum value in the range.
   * 
   * @throw std::runtime_error If the maximum query fails.
   */
  [[nodiscard]] TValue max() {
    return query_builder_.max();
  }

  /**
   * @brief Execute the query.
   * @details Executes the query and returns the result.
   * 
   * @return result_type Result of the query.
   * 
   * @throw std::runtime_error If executing the query fails.
   */
  result_type execute(){
    auto result{query_builder_.execute() |
    std::views::transform([](const auto& entry) {
      return DataPoint<TValue>{
        entry.first.timestamp(),
        entry.first.metric(),
        entry.first.tags(),
        entry.second.value()
      };
    })};
    return {result.begin(), result.end()};
  }

private:
  /**
   * @brief Underlying QueryBuilder.
   * 
   */
  QueryBuilder<TValue> query_builder_;
};
}  // namespace vkdb

#endif // QUERY_FRIENDLY_BUILDER_H