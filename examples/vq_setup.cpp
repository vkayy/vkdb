#include <vkdb/database.h>
#include <iostream>

int main() {
  vkdb::Database db{"global_data"};

  db.createTable("sensors");
  db.createTable("devices");
  auto& sensor_table{db.getTable("sensors")};
  auto& device_table{db.getTable("devices")};

  sensor_table.addTagColumn("location");
  device_table.addTagColumn("location");
  sensor_table.addTagColumn("type");
  device_table.addTagColumn("type");

  std::cout << db.executeFile(
    std::filesystem::current_path() / "../examples/vq_setup.vq"
  );

  db.clear();
}