
# Fenix2TFDINavDataConverter

A Rust tool to convert Fenix NDB data into the TFDI MD-11 navigation data format.

Fenix2TFDINavDataConverter 用 Rust 编写，可将 Fenix 的导航数据库格式转换为 TFDI MD-11 所使用的导航数据格式。

---

## Build / 构建

```bash
cargo build --release
```

---

## Run / 运行

```bash
cargo run --release
```

The program will interactively ask for:
程序会交互式要求输入：

1. Path to NAIP `RTE_SEG.csv`
	NAIP `RTE_SEG.csv` 路径
2. Path to Fenix `nd.db3` database
	Fenix `nd.db3` 数据库路径

After running, the program will automatically detect the installed TFDI MD-11 `Nav-Primary` directory and overwrite it directly.
运行完成后，程序会自动检测本机已安装的 TFDI MD-11 `Nav-Primary` 目录，并直接覆盖写入该目录。

The following locations will be checked in order:
当前会依次尝试以下位置：

1. MSFS2020 (Microsoft Store)
2. MSFS2020 (Steam)
3. MSFS2024 (Microsoft Store)
4. MSFS2024 (Steam)

Before writing `AirwayLegs.json`, the program will first read the existing `Airways.json` and `AirwayLegs.json` in the target directory, and determine which segments need reverse records based on the current airway mirroring mode.
在写入 `AirwayLegs.json` 前，程序会先读取目标目录现有的 `Airways.json` 和 `AirwayLegs.json`，按现有航路镜像模式决定哪些航段需要补写反向记录。

In `--DEBUG` mode, output will still be written to `Nav-Primary_debug/<timestamp>` in the repository, but if an installed `Nav-Primary` is detected, its `Airways.json` and `AirwayLegs.json` will also be used as reference direction templates.
`--DEBUG` 模式仍然输出到仓库内的 `Nav-Primary_debug/<timestamp>`，但如果检测到已安装的 `Nav-Primary`，也会使用其 `Airways.json` 和 `AirwayLegs.json` 作为参考方向模板。
