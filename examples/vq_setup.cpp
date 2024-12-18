#include <vkdb/vq.h>
#include <vkdb/database.h>
#include <vkdb/interpreter.h>
#include <iostream>

int main() {
  vkdb::VQ::runFile(std::filesystem::current_path() / "../examples/vq_setup.vq");
  vkdb::Database db{vkdb::INTERPRETER_DEFAULT_DATABASE};
  db.clear();
}