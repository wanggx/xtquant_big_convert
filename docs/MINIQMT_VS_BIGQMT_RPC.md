# 客户端用法对照（`xtquant_compat` vs MiniQMT）

站在调用方：用

```python
from bigqmt_signal_trader.xtquant_compat import (
    configure,
    xtdata,
    xt_trader,
    XtQuantTrader,
    XtQuantTraderCallback,
    StockAccount,
)
```

代替原来的 `from xtquant import xtdata` / `from xtquant.xttrader import XtQuantTrader`。
下面只谈**函数名、参数、返回值、回调**。

先 `configure(account_id=...)`（或写好客户端配置文件）。`XtQuantTrader` 就是 `BigQmtXtTrader`；模块级 `xt_trader` / `xtdata` 是默认单例。

---

## 1. 保持一致（可当 MiniQMT 写）

名字、大致参数、返回形态对齐。旧业务可以照抄。

### 交易 `xt_trader` / `XtQuantTrader`

`register_callback`、`start` / `connect` / `subscribe` / `stop` / `run_forever`（后四个签名在，语义见第 2 节）

`query_stock_asset`、`query_stock_positions`、`query_stock_position`、`query_stock_orders`、`query_stock_order`、`query_stock_trades`

`order_stock`、`order_stock_async`、`order_stock_batch`

`cancel_order_stock`、`cancel_order_stock_sysid` 及对应 `*_async`

`query_stock_asset_async` 等 `query_*_async(..., callback=)`

回调基类：`on_disconnected`、`on_stock_order`、`on_stock_trade`、`on_order_error`、`on_cancel_error`、`on_order_stock_async_response`、`on_cancel_order_stock_async_response`、`on_account_status`

常量：`STOCK_BUY`、`FIX_PRICE`、`ORDER_SUCCEEDED` 等可从本模块或 `xtquant.xtconstant` shim 导入，值与 MiniQMT 对齐。`StockAccount` 也从这里导入。

### 行情 `xtdata`

`get_full_tick`、`get_instrument_detail` / `get_instrumentdetail`、`get_instrument_type`

`get_market_data`、`get_market_data_ex`、`get_local_data`

`get_stock_list_in_sector`、`get_sector_list`、`get_sector_info`

`subscribe_quote`、`subscribe_quote2`、`subscribe_whole_quote`、`unsubscribe_quote`

`get_trading_dates`、`get_holidays`、`download_holiday_data`

`download_history_data`、`download_history_data2`

`get_divid_factors`、`get_financial_data`、`download_financial_data`、`download_financial_data2`

`get_option_list`、`get_his_option_list`、`get_ipo_info`、`get_etf_info`

`call_formula`、`subscribe_formula`、`unsubscribe_formula`、`get_formula_result`

`datetime_to_timetag` / `timetag_to_datetime`

未单独包一层的 ContextInfo 方法，可用 `xtdata.call_method("名字", ...)`。

---

## 2. 名字在，行为不一致

| 用法 | MiniQMT | 这边 |
|------|---------|------|
| `XtQuantTrader(path, session_id)` | 连本机 MiniQMT | `path` / `session_id` 无意义 |
| `start` / `connect` / `subscribe` | 连 XtQuantServer、订交易推送 | `start` 开本进程的委托/成交回调监听；`connect` 探活并可选同步部署；不连 MiniQMT |
| `configure(account_id=)` | 没有 | **必须先配账号** |
| `order_stock` 返回值 | `int` 委托号，失败 `-1` | 仍可当 `int` 用，同时带合同编号；大 QMT 没有 MiniQMT 那种纯数字委托号 |
| `StockAccount(..., "CREDIT")` | 类型会传到 MiniQMT | **账号类型以 QMT 端配置为准**，客户端声明传不过去 |
| `get_trading_dates` | 毫秒时间戳列表 | 可能是 `'YYYYMMDD'` 字符串，调用方要两种都认 |
| `subscribe_quote(..., period="tick")` | 按品种订 tick | 签名一样；走全推，callback 收增量 dict |
| `subscribe_quote(..., period="1d")` | MiniQMT 推 K 线 | 客户端轮询 K 线 |
| `xtdata.run()` / `xt_trader.run_forever()` | 行情/交易事件循环 | 死循环 `sleep`，用来占住进程；订阅/回调不靠它驱动 |
| `get_stock_type` | 有类型码 | **抛 `NotImplementedError`**，改用 `get_instrument_type` |
| `get_sector_list()` | 真板块列表 | 拿不到时**抛错**；兜底常用名需 `allow_fallback=True` |
| `get_instrument_detail(code, is_detail)` | 双参 | 第二参数忽略 |
| `query_stock_orders` 的 `strategy_name` | MiniQMT 柜台过滤 | 大 QMT 行上没有策略名；只有经本桥下的单会回填 |
| `secu_account` | MiniQMT 常有股东账户 | 经常是 `""`（终端没给） |
| 下单 | 连上就能下 | QMT 端默认关远程下单，需显式打开 |
| `download_history_data*` | MiniQMT 行情服务下载 | 完整交易端常需先在 QMT「数据管理」补数 |

订单/成交对象补了 MiniQMT 风格字段名（含 `m_` 别名）。空字段是「终端没有」，不是没封装。

---

## 3. 新增（MiniQMT 客户端没有）

**接入：** `configure(...)`、`get_default_client()`、`load_client_config()`、`BigQmtRpcClient` / `BigQmtXtData` / `BigQmtXtTrader`

**`xtdata`：** `get_deployment_info`、`call_method`、`get_option_analytics` / `get_option_chain_analytics`（本地 IV/Greeks）、`local_cache_stats`、`stop_all_subscriptions`、以及龙虎榜 / 股东 / 北向 / L2 / BSM / 板块增删 / `get_close_price` 等 ContextInfo 透出。`get_full_tick` 多了 `timeout_seconds`、`types`。

**`xt_trader`：** `sync_deployment`、`describe_trade_detail_fields`、`query_execution_snapshot`、`query_position_statistics`、两融查询一串、`query_ipo_data` / `ipo_subscribe` / `ipo_subscribe_all`、`query_account_infos` / `query_account_status`

---

## 4. MiniQMT 有、这边没有或不要用

| MiniQMT | 这边 |
|---------|------|
| `XtQuantTrader` 连 `path` 下的 MiniQMT / xtdatacenter | 无 |
| 迅投研 token、`xtdata.get_client()`、写 `xtdata.cfg` | 无 |
| MiniQMT 本机逐品种 tick 通道 | 无独立 API；tick 订阅走全推 |
| 回调 `on_connected`、`on_stock_asset`、`on_stock_position` 等 | **基类里没有**，也不会被调用 |
| `get_stock_type` 当可靠分类 | 已改成抛错（大 QMT 恒返回 0） |
| 官方 `xtquant` 里其余冷门函数 | shim **不会**全部实现，调用会 `AttributeError` 或被拒 |

没有从兼容层「删掉」同名函数再换一套名字；日常交易/行情那批名字都还在。缺的是 MiniQMT 进程本身，以及上面这些没有封装的回调和冷门 API。
