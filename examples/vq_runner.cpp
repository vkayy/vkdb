#include <vkdb/vq.h>

int main(int argc, char** argv) {
  if (argc == 1) {
    vkdb::VQ::runPrompt();
  } else if (argc == 2) {
    vkdb::VQ::runFile(argv[1]);
  } else {
    std::cerr << "\033[1;32mUsage: vkdb <.vq filepath>\033[0m\n";
  }
  return 0;
}