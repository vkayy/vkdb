#include <vkdb/database.h>
#include <iostream>

int main() {
  auto db{std::make_unique<vkdb::Database>("wal_replay")};

  db->createTable("sample_table");
  auto& table{db->getTable("sample_table")};
  table.addTagColumn("tag1");

  for (vkdb::Timestamp t{0}; t < 999; ++t) {
    table.query()
      .put(t, "metric", {{"tag1", "value1"}}, 1.0)
      .execute();
  }

  db.reset();

  auto db_replay{std::make_unique<vkdb::Database>("wal_replay")};

  auto& table_replay{db_replay->getTable("sample_table")};
  
  auto sum{table_replay.query()
    .between(0, 999)
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