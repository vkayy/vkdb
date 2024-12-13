#ifndef QUERY_BUILDER_H
#define QUERY_BUILDER_H

#include "utils/concepts.h"
#include "storage/lsm_tree.h"

template <ArithmeticNoCVRefQuals TValue>
class QueryBuilder {
public:
  using key_type = TimeSeriesKey;
  using mapped_type = std::optional<TValue>;
  using value_type = std::pair<const key_type, mapped_type>;
  using result_type = std::vector<value_type>;

  QueryBuilder() = delete;

  explicit QueryBuilder(LSMTree<TValue>& lsm_tree)
    : lsm_tree_{lsm_tree}
    , query_type_{QueryType::None} {}
  
  QueryBuilder(QueryBuilder&&) noexcept = default;
  QueryBuilder& operator=(QueryBuilder&&) noexcept = default;

  QueryBuilder(const QueryBuilder&) = delete;
  QueryBuilder& operator=(const QueryBuilder&) = delete;

  ~QueryBuilder() = default;

  QueryBuilder& point(const key_type& key) {
    if (query_type_ != QueryType::None) {
      throw std::runtime_error{
        "QueryBuilder::point(): Query type already set."
      };
    }
    query_params_ = QueryParams{PointParams{key}};
    query_type_ = QueryType::Point;
    return *this;
  }

  QueryBuilder& range(const key_type& start, const key_type& end) {
    if (query_type_ != QueryType::None) {
      throw std::runtime_error{
        "QueryBuilder::range(): Query type already set."
      };
    }
    query_params_ = QueryParams{RangeParams{start, end}};
    query_type_ = QueryType::Range;
    return *this;
  }

  QueryBuilder& filterByTag(const TagKey& key, const TagValue& value) {
    handle_type_on_filter();
    filters_.emplace_back([key, value](const key_type& k) {
      return k.tags().contains(key) && k.tags().at(key) == value;
    });
    return *this;
  }

  template <AllConvertibleNoCVRefEquals<Tag>... Tags>
  QueryBuilder& filterByAnyTags(const Tags&... tags) {
    handle_type_on_filter();
    filters_.emplace_back([tags...](const key_type& k) {
      return ((k.tags().contains(tags.first) &&
               k.tags().at(tags.first) == tags.second) || ...);
    });
    return *this;
  }

  template <AllConvertibleNoCVRefEquals<Tag>... Tags>
  QueryBuilder& filterByAllTags(const Tags&... tags) {
    handle_type_on_filter();
    filters_.emplace_back([tags...](const key_type& k) {
      return ((k.tags().contains(tags.first) &&
               k.tags().at(tags.first) == tags.second) && ...);
    });
    return *this;
  }

  QueryBuilder& filterByMetric(const Metric& metric) {
    handle_type_on_filter();
    filters_.emplace_back([metric](const key_type& k) {
      return k.metric() == metric;
    });
    return *this;
  }

  template <AllConvertibleNoCVRefEquals<Metric>... Metrics>
  QueryBuilder& filterByAnyMetrics(const Metrics&... metrics) {
    handle_type_on_filter();
    filters_.emplace_back([metrics...](const key_type& k) {
      return ((k.metric() == metrics) || ...);
    });
    return *this;
  }

  QueryBuilder& filterByTimestamp(const Timestamp& timestamp) {
    handle_type_on_filter();
    filters_.emplace_back([timestamp](const key_type& k) {
      return k.timestamp() == timestamp;
    });
    return *this;
  }

  template <AllConvertibleNoCVRefEquals<Timestamp>... Timestamps>
  QueryBuilder& filterByAnyTimestamps(const Timestamps&... timestamps) {
    handle_type_on_filter();
    filters_.emplace_back([timestamps...](const key_type& k) {
      return ((k.timestamp() == timestamps) || ...);
    });
    return *this;
  }

  QueryBuilder& put(const key_type& key, const TValue& value) {
    if (query_type_ != QueryType::None) {
      throw std::runtime_error{
        "QueryBuilder::put(): Query type already set."
      };
    }
    query_params_ = QueryParams{PutParams{key, value}};
    query_type_ = QueryType::Put;
    return *this;
  }

  QueryBuilder& remove(const key_type& key) {
    if (query_type_ != QueryType::None) {
      throw std::runtime_error{
        "QueryBuilder::remove(): Query type already set."
      };
    }
    query_params_ = QueryParams{RemoveParams{key}};
    query_type_ = QueryType::Remove;
    return *this;
  }

  result_type execute() {
    switch (query_type_) {
    case QueryType::None: {
      throw std::runtime_error{
        "QueryBuilder::execute(): Query type not set."
      };
    } case QueryType::Point: {
      auto& params{std::get<PointParams>(query_params_)};
      auto value{lsm_tree_.get(params.key_)};
      if (!value.has_value()) {
        return {};
      }
      return {{params.key_, value}};
    } case QueryType::Range: {
      auto& params{std::get<RangeParams>(query_params_)};
      auto range{lsm_tree_.getRange(params.start_, params.end_)
        | std::views::filter([&](const value_type& entry) {
          return std::all_of(filters_.begin(), filters_.end(),
            [&](const Filter& filter) {
              return filter(entry.first);
            });
        })
      };
      return {range.begin(), range.end()};
    } case QueryType::Put: {
      auto& params{std::get<PutParams>(query_params_)};
      lsm_tree_.put(params.key_, params.value_);
      return {};
    } case QueryType::Remove: {
      auto& params{std::get<RemoveParams>(query_params_)};
      lsm_tree_.remove(params.key_);
      return {};
    } default: {
      throw std::runtime_error{
        "QueryBuilder::execute(): Unknown query type."
      };
    }
    }
  }

private:
  enum class QueryType { None, Point, Range, Put, Remove };
  using Filter = std::function<bool(const key_type&)>;

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

  void handle_type_on_filter() {
    if (query_type_ == QueryType::None) {
      query_params_ = QueryParams{RangeParams{
        MIN_TIME_SERIES_KEY,
        MAX_TIME_SERIES_KEY
      }};
      query_type_ = QueryType::Range;
    }
  }

  LSMTree<TValue>& lsm_tree_;
  QueryType query_type_;
  QueryParams query_params_;
  std::vector<Filter> filters_;
};

#endif // QUERY_BUILDER_H