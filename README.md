# Fenix2TFDINavDataConverter

A Rust tool to convert Fenix NDB data into the TFDI MD-11 navigation data format.

此工具现已重构为 Rust 实现，用于将 Fenix 的导航数据库格式转换为 TFDI MD-11 所使用的导航数据格式。

## Build

```bash
cargo build --release
```

## Run

```bash
cargo run --release
```

程序会交互式要求输入：

1. Fenix `.db3` 数据库路径
2. 起始 `TerminalID`

运行完成后，会在仓库根目录生成 `Nav-Primary.7z`。
