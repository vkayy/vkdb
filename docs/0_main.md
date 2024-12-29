# vkdb

[![c++](https://img.shields.io/badge/C%2B%2B-%2300599C?logo=cplusplus&logoColor=FFFFFF)](https://cplusplus.com/)
[![contributors](https://img.shields.io/github/contributors/vkayy/vkdb)](https://github.com/vkayy/vkdb/graphs/contributors)
![last commit](https://img.shields.io/github/last-commit/vkayy/vkdb)
[![open issues](https://img.shields.io/github/issues/vkayy/vkdb)](https://github.com/vkayy/vkdb/issues/)
[![license](https://img.shields.io/github/license/vkayy/vkdb.svg)](https://github.com/vkayy/vkdb/blob/main/LICENSE)
[![stars](https://img.shields.io/github/stars/vkayy/vkdb)](https://github.com/vkayy/vkdb/stargazers)
[![forks](https://img.shields.io/github/forks/vkayy/vkdb)](https://github.com/vkayy/vkdb/network/members)

```cpp
#include <vkdb/database.h>

int main() {
  vkdb::Database db{"welcome_to_vkdb"};
  db.runFile("made_by_vk.vq");
}
```

**vkdb** is a time series database engine built in C++ with minimal dependencies.

### Motivation

I wanted to challenge myself architecturally and push my boundaries with C++, both in terms of knowledge and performance.

### Links

Author: [Vinz Kakilala](https://linkedin.com/in/vinzkakilala)

Project Link: [vkdb](https://github.com/vkayy/vkdb)


### License

Distributed under the MIT License. See [LICENSE](https://github.com/vkayy/vkdb/blob/main/LICENSE) for more information.

### Credits

Used [MurmurHash3](https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp) for the Bloom filters. Fast, uniform, and deterministic.
