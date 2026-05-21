# Town01 Route Assets

这个目录存放 Town01 route-health 的 tracked canonical 资产。

当前 canonical 文件：

- `town01_canonical_comparison_set.json`
- `town01_random_regression_pool_20260416.json`
- `town01_route_corpus.json`
- `town01_route_corpus_report.md`

说明：

- 这里是 Town01 corpus 的默认读取位置。
- `town01_canonical_comparison_set.json` 是 Town01 默认五条主线样本的 tracked 定义。
- `town01_random_regression_pool_20260416.json` 是当前 nightly A/B 回归固定使用的 6 条随机样本池。
- review、compare、demo、canonical report 应优先消费这份 route-set 资产，而不是各自在脚本里维护硬编码样本。
- repo-root `artifacts/town01_route_corpus.json` 仍保留一段兼容期，避免旧分析脚本立即失效。
- review、comparison、missing-history、batch manifest 仍然属于派生产物，优先写回各自的 batch/run 目录。
