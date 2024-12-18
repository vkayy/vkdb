#include <vkdb/database.h>

int main() {
  vkdb::Database test_db{"test"};

  test_db
    .run("CREATE TABLE temp TAGS tag1, tag2;")
    .runFile(std::filesystem::current_path() / "../examples/vq_setup.vq")
    .runPrompt()
    .clear();
  
  return 0;
}