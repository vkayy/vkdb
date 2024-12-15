#ifndef QUERY_FRIENDLY_BUILDER_H
#define QUERY_FRIENDLY_BUILDER_H

#include "vkdb/builder.h"

namespace vkdb {
template <ArithmeticNoCVRefQuals TValue>
class FriendlyQueryBuilder {
public:
  using size_type = typename QueryBuilder<TValue>::size_type;
  using value_type = DataPoint<TValue>;
  using result_type = std::vector<value_type>;

  FriendlyQueryBuilder() = delete;

  explicit FriendlyQueryBuilder(LSMTree<TValue>& lsm_tree,
                                const TagColumns& tag_columns)
    : query_builder_{QueryBuilder<TValue>(lsm_tree, tag_columns)} {}

  explicit FriendlyQueryBuilder(QueryBuilder<TValue>&& query_builder)
    : query_builder_{std::move(query_builder)} {}

  FriendlyQueryBuilder(FriendlyQueryBuilder&&) noexcept = default;
  FriendlyQueryBuilder& operator=(FriendlyQueryBuilder&&) noexcept = default;

  FriendlyQueryBuilder(const FriendlyQueryBuilder&) = delete;
  FriendlyQueryBuilder& operator=(const FriendlyQueryBuilder&) = delete;

  ~FriendlyQueryBuilder() = default;

  [[nodiscard]] FriendlyQueryBuilder& get(Timestamp timestamp, Metric metric,
                                          const TagTable& tag_table) {
    TimeSeriesKey key{timestamp, metric, tag_table};
    std::ignore = query_builder_.point(key);
    return *this;
  }

  [[nodiscard]] FriendlyQueryBuilder& between(const Timestamp& start, const Timestamp& end) {
    TimeSeriesKey start_key{start, MIN_METRIC, {}};
    TimeSeriesKey end_key{end, MAX_METRIC, {}};
    std::ignore = query_builder_.range(start_key, end_key);
    return *this;
  }

  [[nodiscard]] FriendlyQueryBuilder& whereMetricIs(const Metric& metric) {
    std::ignore = query_builder_.filterByMetric(metric);
    return *this;
  }

  template <AllConvertibleToNoCVRefQuals<Metric>... Metrics>
  [[nodiscard]] FriendlyQueryBuilder& whereMetricIsAnyOf(const Metrics&... metrics) {
    std::ignore = query_builder_.filterByAnyMetrics(metrics...);
    return *this;
  }

  [[nodiscard]] FriendlyQueryBuilder& whereTimestampIs(const Timestamp& timestamp) {
    std::ignore = query_builder_.filterByTimestamp(timestamp);
    return *this;
  }

  template <AllConvertibleToNoCVRefQuals<Timestamp>... Timestamps>
  [[nodiscard]] FriendlyQueryBuilder& whereTimestampIsAnyOf(
    const Timestamps&... timestamps
  ) {
    std::ignore = query_builder_.filterByAnyTimestamps(timestamps...);
    return *this;
  }

  [[nodiscard]] FriendlyQueryBuilder& whereTagsContain(const Tag& tag) {
    std::ignore = query_builder_.filterByTag(tag.first, tag.second);
    return *this;
  }

  template <AllConvertibleToNoCVRefQuals<Tag>... Tags>
  [[nodiscard]] FriendlyQueryBuilder& whereTagsContainAnyOf(
    const Tags&... tags
  ) {
    std::ignore = query_builder_.filterByAnyTags(tags...);
    return *this;
  }

  template <AllConvertibleToNoCVRefQuals<Tag>... Tags>
  [[nodiscard]] FriendlyQueryBuilder& whereTagsContainAllOf(
    const Tags&... tags
  ) {
    std::ignore = query_builder_.filterByAllTags(tags...);
    return *this;
  }

  [[nodiscard]] FriendlyQueryBuilder& put(Timestamp timestamp, Metric metric,
                                          const TagTable& tag_table, TValue value) {
    if (metric.empty() || metric.length() >= TimeSeriesKey::MAX_METRIC_LENGTH) {
      throw std::runtime_error{
        "FriendlyQueryBuilder::get(): Invalid metric."
      };
    }
    TimeSeriesKey key{timestamp, metric, tag_table};
    std::ignore = query_builder_.put(key, value);
    return *this;
  }

  [[nodiscard]] FriendlyQueryBuilder& remove(Timestamp timestamp, Metric metric,
                                             const TagTable& tag_table) {
    TimeSeriesKey key{timestamp, metric, tag_table};
    std::ignore = query_builder_.remove(key);
    return *this;
  }

  [[nodiscard]] size_type count() {
    return query_builder_.count();
  }

  [[nodiscard]] TValue sum() {
    return query_builder_.sum();
  }

  [[nodiscard]] double avg() {
    return query_builder_.avg();
  }

  [[nodiscard]] TValue min() {
    return query_builder_.min();
  }

  [[nodiscard]] TValue max() {
    return query_builder_.max();
  }

  result_type execute() {
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
  QueryBuilder<TValue> query_builder_;
};
}  // namespace vkdb

#endif // QUERY_FRIENDLY_BUILDER_H