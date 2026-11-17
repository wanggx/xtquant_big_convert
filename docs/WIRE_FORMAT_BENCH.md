# Wire-format benchmark：大数据量行情序列化的二进制优化收益

> 用真实形状的 K 线应答对比 4 条 wire 路径的端到端耗时与体积，量化"改成 numpy/arrow 列式二进制后性能提升多少"。
> 测量脚本：[bench_wire_format.py](../bench_wire_format.py)。复现：`python bench_wire_format.py 60 1920 5`。

## 背景

#386 记录 `get_market_data_ex` 大应答的服务端 `to_jsonable` 耗时 ~11s（12.7MB 5m K 线），
60 code 40d 5m 端到端 ~11.5s。慢响应日志 `slow response method=get_market_data_ex total=... to_jsonable=...`。
瓶颈不是 `json.dumps`（那在传输层），而是 **row-oriented 的 `records` 形状**：

- 服务端 `frame.to_dict("records")` 行字典构造（O(n·m)）
- `to_jsonable` 逐值递归类型探测（几百万个值逐个 `type(x) is float`、NaN→None）
- `json.dumps` / `json.loads` 大文本
- 客户端 `pd.DataFrame(list_of_dicts)` 行→列重排

## 测量设置

- 样本：60 个 code，每 code 一个 DataFrame，~1920 行（40 交易日 × 48 根/日 5m）× 11 列
  （`time` 字符串列 + open/high/low/close/amount float64 + volume/openInterest int64 + settle/preClose float64 + suspendFlag int64），
  含 ~1% NaN。合计 **115200 行、1267200 单元格**，对标 #386 的 12.7MB / 60 code 40d 5m。
- 4 条 wire 路径，每条完整复现 编码→bytes→解码：
  - **A1 baseline**：`reset_index + to_dict("records") + to_jsonable` 递归 + JSON + `_restore_jsonable`（忠实复制 [redis_rpc.py:457-519](../src/bigqmt_signal_trader/redis_rpc.py#L457) + [xtquant_compat.py:755-790](../src/bigqmt_signal_trader/xtquant_compat.py#L755)）
  - **A2 列式 JSON**：`to_dict("list")` + JSON；客户端 `pd.DataFrame(dict_of_lists)`
  - **A3 numpy 列式二进制**：每列 `.tobytes()` + JSON 头（dtype/列名/长度）；`np.frombuffer` 重建。**服务端只需 numpy**
  - **A4 pyarrow Arrow IPC**：`pa.ipc.new_stream` / `open_stream.read_pandas`。**需两端 pyarrow**
- 每 path 跑 5 次，报 min/median/max；环境：Python 3.13 + numpy 2.5.5 + pandas 3.1.1 + pyarrow 26.0.1（单线程，OpenBLAS 限 1 线程）。

## 结果（60 code 完整规模）

| 路径 | encode ms (min/med/max) | wire | decode ms (min/med/max) | total (med) | 相对 A1 |
|---|---|---|---|---|---|
| **A1 baseline (records+json)** | 784 / **814** / 901 | **33.0 MB** | 456 / **476** / 541 | **1289.5 ms** | 1.0x |
| A2 column JSON | 528 / **541** / 603 | 18.7 MB | 308 / **319** / 364 | 860.5 ms | 1.5x |
| **A3 numpy columnar binary** | 36 / **41** / 47 | **10.6 MB** | 49 / **55** / 66 | **95.6 ms** | **13.5x** |
| **A4 arrow IPC** | 40 / **42** / 45 | **10.8 MB** | 30 / **35** / 47 | **77.4 ms** | **16.7x** |

## 关键发现

1. **二进制提升 13~17 倍**：A3 numpy 列式二进制 **13.5x**，A4 arrow IPC **16.7x**。这是对用户问题"改成 pyarrow/二进制后性能提升多少"的直接回答。

2. **wire 体积省 3 倍**：A1 JSON 33MB → A3/A4 ~10.7MB。列式二进制无重复列名键、无数值字符串化。

3. **encode 是大头**：A1 encode 814ms（`to_dict("records")` + `to_jsonable` 递归 + `json.dumps`），A3/A4 encode 仅 41~42ms（`.tobytes()` 是 memcpy 级）。decode A1 476ms vs A3/A4 35~55ms。

4. **列式 JSON（A2）只有 1.5x**：因为 `to_jsonable` 递归 + `json.dumps`/`json.loads` 本身的开销没省，列式只省了 `to_dict` 行构造和客户端 `pd.DataFrame` 行→列重排。**证明"只改 wire 形状不上二进制"收益不够**，必须上二进制才能砍掉 JSON 文本往返的开销。

5. **pyarrow 增量收益有限**：A4 vs A3 快 24%（16.7x vs 13.5x），主要在 decode（arrow `read_pandas` 35ms vs numpy `frombuffer + pd.DataFrame` 55ms）。但 **A4 需两端 pyarrow（服务端沙箱不可假定），A3 只需 numpy（服务端内置捆绑）**。考虑可用性，A3 是现实主路径，A4 是"两端恰好都有 pyarrow"的上限快路径。

## 生产外推

- 本机 A1 = 1289.5ms，#386 生产 A1 ~11s —— 偏乐观 8.5x（客户端 pandas 3.1 比 QMT 内置 pandas 0.22 快，符合预期）。
- 相对倍数版本无关（`to_jsonable` 递归是纯 Python，`tobytes`/`frombuffer` 是 memcpy）：**生产 A3 预估 11s / 13.5 ≈ 0.8s，A4 预估 11s / 16.7 ≈ 0.66s**。
- 即从 ~11s 降到 **亚秒级**，且 wire 从 33MB 降到 ~11MB（pipe 消息分片更少、Redis/zmq 带宽更省）。

## 下游兼容性（NaN 直传 vs null）

A1/A2 把 NaN/Inf 转成 JSON null（[redis_rpc.py:470](../src/bigqmt_signal_trader/redis_rpc.py#L470)）；A3/A4 原生承载 NaN（IEEE754 / arrow null 位图）。验证同一帧经两条路径重建后：

- `close.isna().sum()`：A1(null-rebuild)=24 == A3(native)=24 == 源=24 ✓
- `close.dtype`：A1=float64 == A3=float64 ✓
- `df[df["close"] != 0]`（local_cache 的 `_drop_placeholder_rows` 用此过滤）：A1=1920 == A3=1920 ✓（NaN!=0 为 False，两端都不保留 NaN 行）
- `dropna(subset=["close"])`：A1=1896 == A3=1896 ✓

**结论：NaN 原生直传与 null 转换在 `isna()`/`dropna()`/`!= 0` 过滤上行为一致，downstream 安全**。`local_cache` 写 parquet 时 NaN→null（parquet 原生），两条路径产出一致。datetime：现状转字符串、客户端不转回；arrow 原生 timestamp 会改变类型，**落地时需评估下游对 time 列类型的依赖**（本 benchmark time 列已是字符串，A3/A4 原样保留字符串，无类型改变风险）。

## 阶段 B 落地建议

1. **主路径选 A3（numpy 列式二进制）**：服务端只需 numpy（沙箱可用），客户端 numpy 重建；收益 13.5x 已足够（生产 ~11s → ~0.8s）。
2. **A4（arrow IPC）作为探测快路径**：服务端 `try import pyarrow` 成功就用 arrow（再快 24%），否则回落 A3。复用 [local_cache.py:84](../src/bigqmt_signal_trader/local_cache.py#L84) 的 `_pyarrow_available()` 范式。
3. **小 payload 回落 JSON**：纯标量/小应答保持 `to_jsonable`（避免二进制信封开销倒挂）；只对 DataFrame envelope 且行数超过阈值（如 >1000 行）走二进制。
4. **pipe 二进制帧**（主力传输）：帧首加 magic 前缀区分 JSON/二进制帧（pipe message mode 天然保证整帧边界，[pipe_transport.py:143](../src/bigqmt_signal_trader/transports/pipe_transport.py#L143) 最干净），`json.loads` 处加分支。
5. **skew-safe**：老服务端 JSON 帧新客户端识别回落；老客户端见二进制前缀不认识时服务端双发或客户端降级。
6. **datetime 列**：若生产 K 线 time 是 datetime64 而非字符串，A3 需把 datetime64 列走 numpy `datetime64[ns].tobytes()`（frombuffer 同 dtype 重建），A4 arrow 原生 timestamp；下游若依赖字符串 time 需在客户端重建后 strftime。落地前确认生产 time 列 dtype。

## 复现

```bash
cd xtquant_big_convert
# Windows numpy/OpenBLAS 需限单线程避免内存分配失败
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 python bench_wire_format.py 60 1920 5
# 参数：codes  rows  iterations
```

## 阶段 B 落地（已实现）

服务端沙箱实测支持 pyarrow（`init` 探针：`pyarrow 23.0.1` + Arrow IPC round-trip `ok`，2026-11-17），所以主路径直接用 **Arrow IPC（A4，16.7x）**，不再是 numpy（A3）。三级回落保留给没装 pyarrow 的券商终端。

### 配置：`wire_format`

默认 `json`（与历史字节一致，零风险）。要开启 arrow：

- 客户端配置 [bigqmt_signal_trader_client_config.example.py](../src/bigqmt_signal_trader_client_config.example.py) 的 `BIGQMT_REDIS_CONFIG` 加 `"wire_format": "arrow"`
- 或环境变量 `BIGQMT_WIRE_FORMAT=arrow`
- **服务端必须在 `bigqmt_signal_trader_local_config.py` 的 redis 配置块同步设 `wire_format: arrow`**（两端必须一致）

两端不必都开——任一端 `wire_format=json` 时该端就走 JSON。服务端 `wire_format=arrow` 但 transport 非 pipe（redis/zmq）时，自动回落 JSON（只有 pipe 承载二进制帧）。

### 编解码接入点

| 位置 | 作用 |
|---|---|
| [redis_rpc.py](../src/bigqmt_signal_trader/redis_rpc.py) `_arrow_encode_frame` / `_arrow_encode_envelope` | 服务端：DataFrame → Arrow IPC bytes 信封 |
| [redis_rpc.py](../src/bigqmt_signal_trader/redis_rpc.py) `_encode_response_data` | 按 `wire_format` + transport 选 arrow/json，失败回落 json |
| [xtquant_compat.py](../src/bigqmt_signal_trader/xtquant_compat.py) `_restore_arrow_frame` / `_restore_arrow_envelope` | 客户端：Arrow IPC bytes → DataFrame |
| [pipe_transport.py](../src/bigqmt_signal_trader/transports/pipe_transport.py) `_encode_pipe_frame` / `_decode_pipe_frame` | pipe 二进制帧：`BQBIN1` magic + JSON header + arrow bodies |

### 回落链（任一环失败降级 JSON，不报错）

```
wire_format=arrow + transport=pipe + pyarrow 可用 + DataFrame 应答 + 行数≥1000
  → Arrow IPC 编码 → pipe 二进制帧 → 客户端 arrow 解码
任一条件不满足 → to_jsonable + json（与历史完全一致）
```

### 帧格式

- **JSON 帧**：`b'{"...'`（首字节 `{`，与历史字节一致）
- **Arrow 帧**：`b"BQBIN1"` (6) + `json_len` uint32 LE (4) + JSON header + arrow bodies 拼接
  - JSON header 是 response 副本，每个 arrow frame 的 `body` 替换为 `{"_offset", "_length"}` 指向 bodies 区
  - 客户端按 offset/length 切出 body bytes 塞回 envelope，再交 `_restore_jsonable`

### 测试

```bash
python -m unittest tests.bigqmt_signal_trader.test_wire_format_arrow -v
```
6 个用例：arrow round-trip 数据保真、pipe 帧编解码、JSON 帧字节不变、小应答回落、非 DataFrame 回落、编码失败回落。

### 仍待确认（落地实盘前）

- 生产 K 线 `time` 列若为 datetime64 而非字符串，arrow 原生 timestamp 保真——下游若依赖字符串 time（如 local_cache 的 `_time_col` 按字符串名找）需验证。benchmark 已用字符串 time 测，A3/A4 原样保留。
- 实盘跑 60 code 40d 5m，对比 `slow response` 日志 `to_jsonable` 段从 ~11s 降到预期 ~0.66s。
