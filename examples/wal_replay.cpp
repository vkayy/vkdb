#include <vkdb/database.h>
#include <iostream>
#include <chrono>

int main() {
  auto db{std::make_unique<vkdb::Database>("wal_replay")};

  db->createTable("sample_table");
  auto& table{db->getTable("sample_table")};
  table.addTagColumn("tag1");

  for (vkdb::Timestamp t{0}; t < 10'999; ++t) {
    table.query()
      .put(t, "metric", {{"tag1", "value1"}}, 1.0)
      .execute();
  }

  db.reset();

  auto start{std::chrono::high_resolution_clock::now()};
  auto db_replay{std::make_unique<vkdb::Database>("wal_replay")};
  auto end{std::chrono::high_resolution_clock::now()};
  auto elapsed{
    std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
  };

  std::cout << "Database reconstruction time: " << elapsed.count() << "ms\n";

  auto& table_replay{db_replay->getTable("sample_table")};
  
  auto sum{table_replay.query()
    .whereTimestampBetween(10'000, 10'999)
    .whereMetricIs("metric")
    .whereTagsContain({"tag1", "value1"})
    .sum()
  };

  db_replay->clear();

  if (sum != 999) {
    throw std::runtime_error{"WAL replay failed."};
  }

  std::cout << "WAL replay successful.\n";

  return 0;
}