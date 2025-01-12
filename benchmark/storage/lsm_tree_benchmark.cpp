#include <benchmark/benchmark.h>
#include <vkdb/lsm_tree.h>
#include <random>
#include <filesystem>

class LSMTreeBenchmark : public benchmark::Fixture {
protected:
  void SetUp(const benchmark::State& state) override {
    lsm_tree_ = std::make_unique<vkdb::LSMTree<double>>("test_lsm_tree");
  }

  void TearDown(const benchmark::State& state) override {
    lsm_tree_->clear();
    lsm_tree_.reset();
  }

  vkdb::Timestamp random_timestamp(vkdb::Timestamp min, vkdb::Timestamp max) {
    return vkdb::random<vkdb::Timestamp>(min, max);
  }

  double random_value() {
    return vkdb::random<double>(-1000.0, 1000.0);
  }

  vkdb::TimeSeriesKey random_key(vkdb::Timestamp min_time, vkdb::Timestamp max_time) {
    return vkdb::TimeSeriesKey{
      random_timestamp(min_time, max_time),
      std::to_string(random_timestamp(0, 1000)),
      {}
    };
  }

  std::unique_ptr<vkdb::LSMTree<double>> lsm_tree_;
};

BENCHMARK_DEFINE_F(LSMTreeBenchmark, PointWrite)(benchmark::State& state) {
  const auto no_operations{state.range(0)};
  const auto start_time{static_cast<vkdb::Timestamp>(0)};
  
  std::vector<std::pair<vkdb::TimeSeriesKey, double>> data;
  data.reserve(no_operations);
  for (auto i{0}; i < no_operations; ++i) {
    data.emplace_back(
      random_key(start_time, start_time + no_operations),
      random_value()
    );
  }

  for (auto _ : state) {
    state.PauseTiming();
    for (const auto& [key, value] : data) {
      state.ResumeTiming();
      lsm_tree_->put(key, value);
      state.PauseTiming();
    }
    state.ResumeTiming();
  }

  state.SetItemsProcessed(state.iterations() * no_operations);
}

BENCHMARK_DEFINE_F(LSMTreeBenchmark, PointRead)(benchmark::State& state) {
  const auto no_operations{state.range(0)};
  const auto start_time{static_cast<vkdb::Timestamp>(0)};
  
  std::vector<vkdb::TimeSeriesKey> keys;
  keys.reserve(no_operations);
  
  for (auto i{0}; i < no_operations; ++i) {
    auto key{random_key(start_time, start_time + no_operations)};
    keys.push_back(key);
    lsm_tree_->put(key, random_value());
  }

  for (auto _ : state) {
    state.PauseTiming();
    for (const auto& key : keys) {
      state.ResumeTiming();
      benchmark::DoNotOptimize(lsm_tree_->get(key));
      state.PauseTiming();
    }
    state.ResumeTiming();
  }

  state.SetItemsProcessed(state.iterations() * no_operations);
}

BENCHMARK_DEFINE_F(LSMTreeBenchmark, RangeRead)(benchmark::State& state) {
  const auto no_operations{state.range(0)};
  const auto start_time{static_cast<vkdb::Timestamp>(0)};
  const auto time_range{no_operations * 100};
  
  for (auto i{0}; i < no_operations; ++i) {
    lsm_tree_->put(
      random_key(start_time, start_time + time_range),
      random_value()
    );
  }

  const auto range_start{vkdb::TimeSeriesKey{start_time, "0", {}}};
  const auto range_end{vkdb::TimeSeriesKey{start_time + time_range, "1000", {}}};

  for (auto _ : state) {
    benchmark::DoNotOptimize(
      lsm_tree_->getRange(
        range_start,
        range_end,
        [](const auto&){ return true; }
      )
    );
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(LSMTreeBenchmark, PointWrite)
  ->RangeMultiplier(2)
  ->Range(1'000, 10'000)
  ->Range(16'384, 100'000)
  ->Range(131'072, 1'000'000)
  ->Range(1'048'576, 10'000'000)
  ->MinTime(5);

BENCHMARK_REGISTER_F(LSMTreeBenchmark, PointRead)
  ->RangeMultiplier(2)
  ->Range(1'000, 10'000)
  ->Range(16'384, 100'000)
  ->Range(131'072, 1'000'000)
  ->Range(1'048'576, 10'000'000)
  ->MinTime(5);

BENCHMARK_REGISTER_F(LSMTreeBenchmark, RangeRead)
  ->RangeMultiplier(2)
  ->Range(1'000, 10'000)
  ->Range(16'384, 100'000)
  ->Range(131'072, 1'000'000)
  ->Range(1'048'576, 10'000'000)
  ->MinTime(5);
