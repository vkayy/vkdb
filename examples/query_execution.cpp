#include <vkdb/database.h>
#include <iostream>

int main() {
  vkdb::Database db{"sensor_data"};
  
  db.createTable("atmospheric");
  auto& table{db.getTable("atmospheric")};
  
  table.addTagColumn("region");
  table.addTagColumn("city");
  
  db.executeFile(
    std::filesystem::current_path() / "../examples/atmospheric.vq"
  );
  
  std::cout
    << "Average temperature in Europe: "
    << db.executeQuery(
      "SELECT AVG temperature "
      "FROM atmospheric "
      "ALL "
      "WHERE region=eu"
    ) << "°C\n";

  std::cout
    << "Total rainfall in Asia: "
    << db.executeQuery(
      "SELECT SUM rainfall "
      "FROM atmospheric "
      "ALL "
      "WHERE region=as"
    ) << "mm\n";

  std::cout
    << "Number of data points in North America "
    << "from 1'702'550'000 to 1'702'650'000: "
    << db.executeQuery(
      "SELECT COUNT temperature "
      "FROM atmospheric "
      "BETWEEN 1702550000 AND 1702650000 "
      "WHERE region=na"
    ) << "\n";

  db.clear();

  return 0;
}
