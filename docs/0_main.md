# vkdb

![](images/vkdb-full-cropped.png)

<div style="margin: 32px;" align="center">
[![c++](https://img.shields.io/badge/C%2B%2B-%2300599C?logo=cplusplus&logoColor=FFFFFF)](https://cplusplus.com/)
[![contributors](https://img.shields.io/github/contributors/vkayy/vkdb)](https://github.com/vkayy/vkdb/graphs/contributors)
![last commit](https://img.shields.io/github/last-commit/vkayy/vkdb)
[![open issues](https://img.shields.io/github/issues/vkayy/vkdb)](https://github.com/vkayy/vkdb/issues/)
[![license](https://img.shields.io/github/license/vkayy/vkdb.svg)](https://github.com/vkayy/vkdb/blob/main/LICENSE)
[![stars](https://img.shields.io/github/stars/vkayy/vkdb)](https://github.com/vkayy/vkdb/stargazers)
[![forks](https://img.shields.io/github/forks/vkayy/vkdb)](https://github.com/vkayy/vkdb/network/members)
</div>

> **⚠ Warning**<br> vkdb is currently in the early stages of development and is not yet ready for daily use!


**vkdb** is a hobbyist time series database engine built with a focus on simplicity and modernity. Motivated by unfamiliar architectures and endless optimisation opportunities, this project is far from commercial, and is defined by a pursuit of challenge.


```cpp
#include <vkdb/database.h>

int main() {
  vkdb::Database db{"welcome_to_vkdb"};
  db.runFile("made_by_vk.vq");
}
```

#### Links

Author: [Vinz Kakilala](https://linkedin.com/in/vinzkakilala)

Project Link: [vkdb](https://github.com/vkayy/vkdb)

#### License

Distributed under the MIT License. See [LICENSE](https://github.com/vkayy/vkdb/blob/main/LICENSE) for more information.

#### Credits

Used [MurmurHash3](https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp) for the Bloom filters. Fast, uniform, and deterministic.
