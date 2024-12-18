#include <vkdb/database.h>
#include <vkdb/vq.h>
#include <iostream>

int main() {
  vkdb::VQ::run(
    "CREATE TABLE atmospheric "
    "TAGS region, city;"
  );

  vkdb::VQ::runFile(
    std::filesystem::current_path() / "../examples/query_execution.vq"
  );
  
  std::cout << "Average temperature in Europe: ";
  vkdb::VQ::run(
    "SELECT AVG temperature "
    "FROM atmospheric "
    "ALL "
    "WHERE region=eu;"
  );

  std::cout << "Total rainfall in Asia: ";
  vkdb::VQ::run(
    "SELECT SUM rainfall "
    "FROM atmospheric "
    "ALL "
    "WHERE region=as;"
  );

  std::cout << "Number of data points in North America "
    << "from 1'702'550'000 to 1'702'650'000: ";
  vkdb::VQ::run(
    "SELECT COUNT temperature "
    "FROM atmospheric "
    "BETWEEN 1702550000 AND 1702650000 "
    "WHERE region=na;"
  );

  vkdb::Database{vkdb::INTERPRETER_DEFAULT_DATABASE}.clear();
}
