#include <vkdb/database.h>
#include <iostream>

int main() {
  vkdb::Database db{"global_data"};
  
  std::cout << db.executeFile(
    std::filesystem::current_path() / "../examples/vq_setup.vq"
  );

  db.clear();
}