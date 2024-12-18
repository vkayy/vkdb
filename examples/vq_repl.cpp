#include <vkdb/vq.h>

int main() {
  vkdb::VQ vq;
  vq.runPrompt();
  vkdb::Database db{vkdb::INTERPRETER_DEFAULT_DATABASE};
  db.clear();
}