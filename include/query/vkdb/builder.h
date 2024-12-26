#ifndef QUERY_BUILDER_H
#define QUERY_BUILDER_H

#include <vkdb/concepts.h>
#include <vkdb/lsm_tree.h>
#include <ranges>
#include <unordered_set>

namespace vkdb {
using TagColumns = std::unordered_set<TagKey>;

template <ArithmeticNoCVRefQuals TValue>
class QueryBuilder {
public:
  using keytype = TimeSeriesKey;
  using mapped_type = std::optional<TValue>;
  using valuetype = std::pair<const keytype, mapped_type>;
  using size_type = uint64_t;
  using result_type = std::vector<valuetype>;

  QueryBuilder() = delete;

  explicit QueryBuilder(
    LSMTree<TValue>& lsm_tree,
    const TagColumns& tag_columns
  )
    : lsm_tree_{lsm_tree}
    , tag_columns_{tag_columns}
    , query_type_{QueryType::None}
    , filters_{TRUE_TIME_SERIES_KEY_FILTER} {}
  
  QueryBuilder(QueryBuilder&&) noexcept = default;
  QueryBuilder& operator=(QueryBuilder&&) noexcept = default;

  QueryBuilder(const QueryBuilder&) noexcept = default;
  QueryBuilder& operator=(const QueryBuilder&) noexcept = default;

  ~QueryBuilder() = default;

  [[nodiscard]] QueryBuilder& point(const keytype& key) {
    validate_tags(key.tags());
    set_query_type(QueryType::Point);
    query_params_ = QueryParams{PointParams{key}};
    return *this;
  }

  [[nodiscard]] QueryBuilder& range(const keytype& start, const keytype& end) {
    validate_tags(start.tags());
    validate_tags(end.tags());
    set_query_type(QueryType::Range);
    query_params_ = QueryParams{RangeParams{start, end}};
    return *this;
  }

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

  template <AllConvertibleToNoCVRefQuals<Tag>... Tags>
  [[nodiscard]] QueryBuilder& filterByAnyTags(const Tags&... tags) {
    validate_tags(tags...);
    add_filter([tags...](const auto& k) {
      return ((k.tags().contains(tags.first) &&
               k.tags().at(tags.first) == tags.second) || ...);
    });
    return *this;
  }

  template <AllConvertibleToNoCVRefQuals<Tag>... Tags>
  [[nodiscard]] QueryBuilder& filterByAllTags(const Tags&... tags) {
    validate_tags(tags...);
    add_filter([tags...](const auto& k) {
      return ((k.tags().contains(tags.first) &&
               k.tags().at(tags.first) == tags.second) && ...);
    });
    return *this;
  }

  [[nodiscard]] QueryBuilder& filterByMetric(const Metric& metric) {
    add_filter([metric](const auto& k) {
      return k.metric() == metric;
    });
    return *this;
  }

  template <AllConvertibleToNoCVRefQuals<Metric>... Metrics>
  [[nodiscard]] QueryBuilder& filterByAnyMetrics(const Metrics&... metrics) {
    add_filter([metrics...](const auto& k) {
      return ((k.metric() == metrics) || ...);
    });
    return *this;
  }

  [[nodiscard]] QueryBuilder& filterByTimestamp(const Timestamp& timestamp) {
    add_filter([timestamp](const auto& k) {
      return k.timestamp() == timestamp;
    });
    return *this;
  }

  template <AllConvertibleToNoCVRefQuals<Timestamp>... Timestamps>
  [[nodiscard]] QueryBuilder& filterByAnyTimestamps(
    const Timestamps&... timestamps
  ) {
    add_filter([timestamps...](const auto& k) {
      return ((k.timestamp() == timestamps) || ...);
    });
    return *this;
  }

  [[nodiscard]] QueryBuilder& put(const keytype& key, const TValue& value) {
    validate_tags(key.tags());
    set_query_type(QueryType::Put);
    query_params_ = QueryParams{PutParams{key, value}};
    return *this;
  }

  [[nodiscard]] QueryBuilder& remove(const keytype& key) {
    validate_tags(key.tags());
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
      [](const TValue& acc, const valuetype& entry) {
        return acc + entry.second.value();
      });
  }

  [[nodiscard]] double avg() {
    setup_aggregate();
    auto range{get_nonempty_filtered_range()};
    auto sum{std::accumulate(range.begin(), range.end(), TValue{},
      [](const auto& acc, const auto& entry) {
        return acc + entry.second.value();
      })};
    return static_cast<double>(sum) / std::ranges::distance(range);
  }

  [[nodiscard]] TValue min() {
    setup_aggregate();
    auto range{get_nonempty_filtered_range()};
    return std::ranges::min_element(range, {}, [](const auto& entry) {
      return entry.second.value();
    })->second.value();
  }

  [[nodiscard]] TValue max() {
    setup_aggregate();
    auto range{get_nonempty_filtered_range()};
    return std::ranges::max_element(range, {}, [](const auto& entry) {
      return entry.second.value();
    })->second.value();
  }

  result_type execute() {
    switch (query_type_) {
    case QueryType::None:
      if (filters_.size() == 1) {
        throw std::runtime_error{
          "QueryBuilder::execute(): No query type specified."
        };
      }
      set_default_range_if_none();
      return get_filtered_range();
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
    keytype key;
  };

  struct RangeParams {
    keytype start;
    keytype end;
  };

  struct PutParams {
    keytype key;
    TValue value;
  };

  struct RemoveParams {
    keytype key;
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
        "QueryBuilder::check_if_aggregable(): Query type must be aggregable."
      };
    }
  }

  void setup_aggregate() {
    set_default_range_if_none();
    check_if_aggregable();
  }

  void add_filter(TimeSeriesKeyFilter&& filter) {
    filters_.push_back(std::move(filter));
  }

  [[nodiscard]] result_type get_filtered_range() const {
    if (query_type_ == QueryType::Point) {
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

  [[nodiscard]] result_type execute_point_query() const {
    const auto& params{std::get<PointParams>(query_params_)};
    auto value{lsm_tree_.get(params.key)};
    if (!value.has_value()) {
      return {};
    }
    return {{params.key, value}};
  }

  [[nodiscard]] result_type execute_range_query() const {
    auto range{get_filtered_range()};
    return {range.begin(), range.end()};
  }

  [[nodiscard]] result_type execute_put_query() {
    const auto& params{std::get<PutParams>(query_params_)};
    lsm_tree_.put(params.key, params.value);
    return {};
  }

  [[nodiscard]] result_type execute_remove_query() {
    const auto& params{std::get<RemoveParams>(query_params_)};
    lsm_tree_.remove(params.key);
    return {};
  }

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

  LSMTree<TValue>& lsm_tree_;
  const TagColumns& tag_columns_;
  QueryType query_type_;
  QueryParams query_params_;
  std::vector<TimeSeriesKeyFilter> filters_;
};
}  // namespace vkdb

#endif // QUERY_BUILDER_H