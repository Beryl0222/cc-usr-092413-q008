# 豆类结构配方放大

服务用于追踪豆类冷冻结构化实验和中试执行差异，使配方、设备与质构结果可以稳定比较。

项目当前提供稳定的基础服务入口，便于本地联调和运维巡检。运行 `python3 service.py --check` 可核对服务配置；执行 `python3 service.py --port 8000` 后访问 `/health` 可确认服务身份。

## 共享产线过敏原换线放行

中试工厂在同一条结构化产线上轮换豌豆、蚕豆与含麸质辅料。服务把换线放行从纸面清场记录升级为与真实生产顺序、设备段和待放行批次一一对应的领域流程（`changeover.py`）：

- **计划源于真实顺序**：换线计划只能由产线的真实生产顺序生成，按产线段登记上一批物料、拆洗步骤、清洁验证样本、检测限与下一批标签声明；相邻批次存在"上一批携带且下一批标签未声明"的过敏原时生成计划并开启清洁窗口，否则批次共用当前窗口。
- **三级留痕放行**：操作员完成拆洗步骤，实验室签收样本并出具结果，质量授权人只对覆盖完整且检测满足阈值（结果 ≤ 检测限）的批次放行；清洁结论、配方版本与标签快照在单一事务中一起生效，服务恢复后不会重复放行。
- **失败收紧**：抽样失败会隔离同一清洁窗口内尚未出厂的相关批次，已出厂部分生成可追踪的风险处置（含通知范围），原检验记录不可改写；同一采样编号的相同重传复用结果，异内容立即冻结。
- **范围重算**：返工、设备旁路与计划外插单都会重新计算受影响范围，已失效计划下的已放行批次重新隔离、已出厂批次补建风险处置。
- **全程可溯**：`GET /batches/{id}/trace` 从成品反查换线责任（操作员/实验室/质量授权人）、样本证据、例外批准及通知范围。

### 主要接口

| 方法与路径 | 说明 |
| --- | --- |
| `POST /materials`、`POST /formula-versions`、`POST /lines` | 登记物料（含过敏原）、配方版本（含标签声明）、产线及产线段（含拆洗步骤模板与检测限） |
| `POST /lines/{id}/sequence` | 批次追加到真实生产顺序末尾 |
| `POST /lines/{id}/sequence:insert` | 计划外插单并重算受影响范围 |
| `POST /lines/{id}/plans:generate`、`GET /lines/{id}/plans`、`GET /plans/{id}` | 从真实顺序生成/查看换线计划 |
| `POST /plans/{id}/steps/{step}/complete` | 操作员完成拆洗步骤 |
| `POST /plans/{id}/samples`、`POST /samples/{no}/receipt`、`POST /samples/{no}/results` | 登记补充样本、实验室签收、出具结果（同编号幂等，异内容冻结） |
| `POST /batches/{id}/complete`、`/release`、`/ship`、`/rework` | 完工、质量授权人放行（幂等）、出厂、返工重算 |
| `POST /plans/{id}/segments/{seg}/bypass` | 设备旁路，计划失效并重算范围 |
| `POST /isolations/{id}/exception-approvals` | 隔离批次例外批准放行 |
| `POST /dispositions/{id}/notifications` | 登记风险处置通知对象 |
| `GET /batches/{id}/trace`、`GET /windows/{id}` | 成品反查全链路证据 |

持久化使用 SQLite，默认数据库文件 `changeover.db`，可用 `--db` 或环境变量 `CHANGEOVER_DB` 指定。

## 测试与构建

执行完整测试：

```bash
npm test
```

执行编译检查：

```bash
python3 -m compileall -q .
```

两条命令都可在单个 Linux 应用容器内直接运行，不需要额外服务。
