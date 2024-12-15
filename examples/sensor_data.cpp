#include "vkdb/database.h"
#include "vkdb/random.h"
#include <iostream>

int main() {
  vkdb::Database db{"sensor_data"};

  db.createTable("atmospheric");

  auto& table{db.getTable("atmospheric")};

  vkdb::Metric temp_metric{"temperature"};
  vkdb::Metric humidity_metric{"humidity"};
  vkdb::Tag region_eu_tag{"region", "eu"};
  vkdb::Tag city_london_tag{"city", "london"};

  table.addTagColumn("region");
  table.addTagColumn("city");
  
  for (vkdb::Timestamp t{0}; t < 10'000; ++t) {
    double temperature{vkdb::random<int>(0, 400) / 10.0};
    double humidity{vkdb::random<int>(0, 1000) / 10.0};
    table.query()
      .put(t, temp_metric, {region_eu_tag, city_london_tag}, temperature)
      .execute();
    table.query()
      .put(t, humidity_metric, {region_eu_tag, city_london_tag}, humidity)
      .execute();
  }

  auto average_temperature{table.query()
    .between(2'500, 7'500)
    .whereMetricIs(temp_metric)
    .whereTagsContainAllOf(region_eu_tag, city_london_tag)
    .avg()
  };

  auto max_humidity{table.query()
    .between(1'000, 3'000)
    .whereMetricIs(humidity_metric)
    .whereTagsContainAllOf(region_eu_tag, city_london_tag)
    .max()
  };

  std::cout << "Average temperature between T2500 and T7500: ";
  std::cout << average_temperature << "C\n";
  std::cout << "Max humidity between T1000 and T3000: ";
  std::cout << max_humidity << "%\n";

  db.clear();

  return 0;
}