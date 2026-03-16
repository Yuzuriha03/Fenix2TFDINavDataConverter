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

运行完成后，程序会自动检测本机已安装的 TFDI MD-11 `Nav-Primary` 目录，并直接覆盖写入该目录。

当前会依次尝试以下位置：

1. MSFS2020 (Microsoft Store)
2. MSFS2020 (Steam)
3. MSFS2024 (Microsoft Store)
4. MSFS2024 (Steam)

在写入 `AirwayLegs.json` 前，程序会先读取目标目录现有的 `Airways.json` 和 `AirwayLegs.json`，按现有航路镜像模式决定哪些航段需要补写反向记录。

`--DEBUG` 模式仍然输出到仓库内的 `Nav-Primary_debug/<timestamp>`，但如果检测到已安装的 `Nav-Primary`，也会使用其 `Airways.json` 和 `AirwayLegs.json` 作为参考方向模板。
