#include <vkdb/vq.h>
#include <iostream>

int main() {
  vkdb::VQ vq;
  vq.runFile(std::filesystem::current_path() / "../examples/vq_setup.vq");
  vkdb::Database db{vkdb::INTERPRETER_DEFAULT_DATABASE};
  db.clear();
}