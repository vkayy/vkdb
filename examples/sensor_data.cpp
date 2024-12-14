#include "database/database.h"
#include "utils/random.h"
#include <iostream>

int main() {
  vkdb::Database db;

  db.createTable("sensor_data");

  auto& table{db.getTable("sensor_data")};

  table.addTagColumn("region");
  table.addTagColumn("city");
  
  for (vkdb::Timestamp i{0}; i < 10'000; ++i) {
    auto t{static_cast<double>(vkdb::random<int>(0, 400)) / 10.0};
    auto h{static_cast<double>(vkdb::random<int>(0, 1000)) / 10.0};
    table.query()
      .put(i, "temperature", {{"region", "eu"}, {"city", "london"}}, t)
      .execute();
    table.query()
      .put(i, "humidity", {{"region", "eu"}, {"city", "london"}}, h)
      .execute();
  }

  auto result{table.query()
    .between(2'500, 7'500)
    .whereMetricIs("temperature")
    .whereTagsContainAllOf(
      std::make_pair("region", "eu"),
      std::make_pair("city", "london")
    )
    .avg()
  };

  std::cout << "Average temperature between T2500 and T7500: " << result << std::endl;

  return 0;
}