#ifndef QUERY_BUILDER_H
#define QUERY_BUILDER_H

#include "utils/concepts.h"
#include "storage/lsm_tree.h"
#include <ranges>

template <ArithmeticNoCVRefQuals TValue>
class QueryBuilder {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = std::optional<TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using size_type = uint64_t;
  using result_type = std::vector<value_type>;

  QueryBuilder() = delete;

  explicit QueryBuilder(LSMTree<TValue>& lsm_tree)
    : lsm_tree_{lsm_tree}
    , query_type_{QueryType::None}
    , combined_filter_{TRUE_TIME_SERIES_KEY_FILTER} {}
  
  QueryBuilder(QueryBuilder&&) noexcept = default;
  QueryBuilder& operator=(QueryBuilder&&) noexcept = default;

  QueryBuilder(const QueryBuilder&) = delete;
  QueryBuilder& operator=(const QueryBuilder&) = delete;

  ~QueryBuilder() = default;

  [[nodiscard]] QueryBuilder& point(const key_type& key) {
    set_query_type(QueryType::Point);
    query_params_ = QueryParams{PointParams{key}};
    return *this;
  }

  [[nodiscard]] QueryBuilder& range(const key_type& start, const key_type& end) {
    set_query_type(QueryType::Range);
    query_params_ = QueryParams{RangeParams{start, end}};
    return *this;
  }

  [[nodiscard]] QueryBuilder& filterByTag(const TagKey& key, const TagValue& value) {
    set_default_range_if_none();
    add_filter([key, value](const key_type& k) {
      return k.tags().contains(key) && k.tags().at(key) == value;
    });
    return *this;
  }

  template <AllConvertibleNoCVRefEquals<Tag>... Tags>
  [[nodiscard]] QueryBuilder& filterByAnyTags(const Tags&... tags) {
    set_default_range_if_none();
    add_filter([tags...](const key_type& k) {
      return ((k.tags().contains(tags.first) &&
               k.tags().at(tags.first) == tags.second) || ...);
    });
    return *this;
  }

  template <AllConvertibleNoCVRefEquals<Tag>... Tags>
  [[nodiscard]] QueryBuilder& filterByAllTags(const Tags&... tags) {
    set_default_range_if_none();
    add_filter([tags...](const key_type& k) {
      return ((k.tags().contains(tags.first) &&
               k.tags().at(tags.first) == tags.second) && ...);
    });
    return *this;
  }

  [[nodiscard]] QueryBuilder& filterByMetric(const Metric& metric) {
    set_default_range_if_none();
    add_filter([metric](const key_type& k) {
      return k.metric() == metric;
    });
    return *this;
  }

  template <AllConvertibleNoCVRefEquals<Metric>... Metrics>
  [[nodiscard]] QueryBuilder& filterByAnyMetrics(const Metrics&... metrics) {
    set_default_range_if_none();
    add_filter([metrics...](const key_type& k) {
      return ((k.metric() == metrics) || ...);
    });
    return *this;
  }

  [[nodiscard]] QueryBuilder& filterByTimestamp(const Timestamp& timestamp) {
    set_default_range_if_none();
    add_filter([timestamp](const key_type& k) {
      return k.timestamp() == timestamp;
    });
    return *this;
  }

  template <AllConvertibleNoCVRefEquals<Timestamp>... Timestamps>
  [[nodiscard]] QueryBuilder& filterByAnyTimestamps(const Timestamps&... timestamps) {
    set_default_range_if_none();
    add_filter([timestamps...](const key_type& k) {
      return ((k.timestamp() == timestamps) || ...);
    });
    return *this;
  }

  [[nodiscard]] QueryBuilder& put(const key_type& key, const TValue& value) {
    set_query_type(QueryType::Put);
    query_params_ = QueryParams{PutParams{key, value}};
    return *this;
  }

  [[nodiscard]] QueryBuilder& remove(const key_type& key) {
    set_query_type(QueryType::Remove);
    query_params_ = QueryParams{RemoveParams{key}};
    return *this;
  }

  [[nodiscard]] size_type count() {
    setup_aggregate();
    return std::ranges::distance(get_filtered_range());
  }

  [[nodiscard]] TValue sum() {
    setup_aggregate();
    auto range{get_nonempty_filtered_range()};
    return std::accumulate(range.begin(), range.end(), TValue{},
      [](const TValue& acc, const value_type& entry) {
        return acc + entry.second.value();
      });
  }

  [[nodiscard]] double avg() {
    setup_aggregate();
    auto range{get_nonempty_filtered_range()};
    auto sum{std::accumulate(range.begin(), range.end(), TValue{},
      [](const TValue& acc, const value_type& entry) {
        return acc + entry.second.value();
      })};
    return static_cast<double>(sum) / std::ranges::distance(range);
  }

  [[nodiscard]] TValue min() {
    setup_aggregate();
    auto range{get_nonempty_filtered_range()};
    return std::ranges::min_element(range, {}, [](const value_type& entry) {
      return entry.second.value();
    })->second.value();
  }

  [[nodiscard]] TValue max() {
    setup_aggregate();
    auto range{get_nonempty_filtered_range()};
    return std::ranges::max_element(range, {}, [](const value_type& entry) {
      return entry.second.value();
    })->second.value();
  }

  result_type execute() {
    switch (query_type_) {
    case QueryType::None:
      throw std::runtime_error{"QueryBuilder::execute(): Query type not set."};
    case QueryType::Point:
      return execute_point_query();
    case QueryType::Range:
      return execute_range_query();
    case QueryType::Put:
      return execute_put_query();
    case QueryType::Remove:
      return execute_remove_query();
    }
  }

private:
  enum class QueryType { None, Point, Range, Put, Remove };

  struct PointParams {
    key_type key_;
  };

  struct RangeParams {
    key_type start_;
    key_type end_;
  };

  struct PutParams {
    key_type key_;
    TValue value_;
  };

  struct RemoveParams {
    key_type key_;
  };

  using QueryParams = std::variant<
    std::monostate,
    PointParams,
    RangeParams,
    PutParams,
    RemoveParams
  >;

  void set_query_type(QueryType query_type) {
    if (query_type_ != QueryType::None) {
      throw std::runtime_error{
        "QueryBuilder::set_query_type(): Query type already set."
      };
    }
    query_type_ = query_type;
  }

  void set_default_range_if_none() noexcept {
    if (query_type_ == QueryType::None) {
      query_params_ = QueryParams{RangeParams{
        MIN_TIME_SERIES_KEY,
        MAX_TIME_SERIES_KEY
      }};
      query_type_ = QueryType::Range;
    }
  }

  void check_if_aggregable() const {
    if (query_type_ != QueryType::Range && query_type_ != QueryType::Point) {
      throw std::runtime_error{
        "QueryBuilder::check_if_aggregable(): Query type must be aggregatable."
      };
    }
  }

  void setup_aggregate() {
    set_default_range_if_none();
    check_if_aggregable();
  }

  void add_filter(TimeSeriesKeyFilter&& filter) {
    combined_filter_ = [filter, combined_filter{std::move(combined_filter_)}]
      (const key_type& key) {
      return filter(key) && combined_filter(key);
    };
  }

  [[nodiscard]] result_type get_filtered_range() const {
    if (query_type_ == QueryType::Point) {
      return execute_point_query();
    }
    const auto& params{std::get<RangeParams>(query_params_)};
    return lsm_tree_.getRange(params.start_, params.end_, combined_filter_);
  }

  [[nodiscard]] result_type get_nonempty_filtered_range() const {
    auto filtered_range{get_filtered_range()};
    if (filtered_range.empty()) {
      throw std::runtime_error{
        "QueryBuilder::get_nonempty_filtered_range(): Empty range."
      };
    }
    return filtered_range;
  }

  [[nodiscard]] result_type execute_point_query() const {
    const auto& params{std::get<PointParams>(query_params_)};
    auto value{lsm_tree_.get(params.key_)};
    if (!value.has_value()) {
      return {};
    }
    return {{params.key_, value}};
  }

  [[nodiscard]] result_type execute_range_query() const {
    auto range{get_filtered_range()};
    return {range.begin(), range.end()};
  }

  [[nodiscard]] result_type execute_put_query() {
    const auto& params{std::get<PutParams>(query_params_)};
    lsm_tree_.put(params.key_, params.value_);
    return {};
  }

  [[nodiscard]] result_type execute_remove_query() {
    const auto& params{std::get<RemoveParams>(query_params_)};
    lsm_tree_.remove(params.key_);
    return {};
  }

  LSMTree<TValue>& lsm_tree_;
  QueryType query_type_;
  QueryParams query_params_;
  TimeSeriesKeyFilter combined_filter_;
};

#endif // QUERY_BUILDER_H