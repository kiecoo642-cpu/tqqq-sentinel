# TQQQ-LRS 状态翻转事件告警系统

基于 Michael A. Gayed & Charles V. Bilello 经典论文《Leverage for the Long Run》实现的无服务器（Serverless）量化信号监控系统。

## 核心特性

- **静默监控，变盘即报**：日常状态不变时零打扰（Zero-Notification），仅在 QQQ 复权收盘价有效穿刺 200 日均线（SMA200）、发生多空状态翻转时发送高优先级告警。
- **大厂 IM 机器人自适应网关（永不过期）**：
  - 只需配置一个 `WEBHOOK_URL`，系统**根据域名自动识别「飞书 / 企业微信 / 钉钉」**，自动渲染对应平台的原生彩色交互卡片/Markdown 并附带 `@所有人` 强提醒。
  - 支持钉钉/飞书 HMAC-SHA256 安全加签（`WEBHOOK_SECRET`），支持同时配置多个群机器人并发推送。
- **严谨复权与容错防抖**：
  - 使用 QQQ 官方复权收盘价（`auto_adjust=True`）计算 200MA，内置盘中未收盘 K 线剥离、重复日期去重与 3 次网络重试。
  - **防漏报机制**：仅在推送接口确认返回成功（`errcode == 0` / `code == 0`）后才更新 `state.json`；若推送失败则主动报错退出，等待下次重试。

## 目录结构

```text
tqqq-sentinel/
├── .github/
│   └── workflows/
│       └── monitor.yml   # GitHub Actions 定时任务配置（北京时间周二至周六 07:30）
├── main.py               # 核心调度、数据抓取、状态机判定与多平台 Webhook 网关
├── test_main.py          # 全场景自动化单元测试与真实历史回放套件
├── state.json            # 状态缓存文件（记录当前多空状态与基准数据）
├── requirements.txt      # 锁定范围的核心依赖 (yfinance, pandas, requests)
└── README.md             # 部署与群机器人接入指南
```

## 群机器人 Webhook 接入指南（三选一，3 分钟搞定）

在手机或电脑端建一个**只有你自己的 1 人内部群**（如「TQQQ 哨兵告警群」），按下面任一平台添加自定义机器人：

### 方案 A：企业微信（手机微信可直接接收！）
1. 在企业微信电脑端右键群聊 -> **添加群机器人** -> 新建机器人 -> 复制 `Webhook 地址`（形如 `https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...`）。
2. **微信联动秘籍**：进入「企业微信后台 -> 我的企业 -> 微信插件」，用个人微信扫码关注。此后群机器人的告警会**直接同步弹窗到你的个人微信聊天列表**！

### 方案 B：飞书（卡片颜值最高，红/橙高亮标题栏）
1. 在飞书群设置 -> **群机器人** -> **添加机器人** -> 选择 **自定义机器人**。
2. 复制 `Webhook 地址`（形如 `https://open.feishu.cn/open-apis/bot/v2/hook/...`）。
3. 安全设置可保持默认，或勾选「签名校验」并将密钥填入 `WEBHOOK_SECRET`。

### 方案 C：钉钉（支持关键词或加签）
1. 在钉钉群设置 -> **智能群助手** -> **添加机器人** -> **自定义（通过 Webhook 接入）**。
2. 安全设置可选择：
   - **自定义关键词**：填入 `QQQ` 或 `指令`（所有告警均自带该词，免配密钥）；
   - 或 **加签**：复制 `SEC...` 开头的密钥填入 GitHub Secrets 的 `WEBHOOK_SECRET`。
3. 复制生成的 `Webhook 地址`（形如 `https://oapi.dingtalk.com/robot/send?access_token=...`）。

---

## GitHub Actions 部署步骤

1. 将本项目推送到你的 GitHub 仓库（公开或私有均可）。
2. 进入仓库 -> `Settings` -> `Secrets and variables` -> `Actions` -> `New repository secret`：
   - **必填**：Name 填 `WEBHOOK_URL`，Secret 粘贴你上面复制的飞书 / 企业微信 / 钉钉 Webhook 链接（若想同时推多个群，用英文逗号 `,` 分隔多个链接即可）。
   - **选填**：若你在钉钉或飞书开启了加签，新建 `WEBHOOK_SECRET` 填入加签密钥。

## 验收测试方法

1. 本地或云端运行 `python main.py`，首次生成 `state.json`。
2. 手动将 `state.json` 中的 `"last_state"` 改为相反状态（例如 `"LONG"` 改为 `"CASH"`）。
3. 注入环境变量测试真实群推送：
   ```powershell
   $env:WEBHOOK_URL="你的群机器人Webhook地址"
   .\.venv\Scripts\python.exe main.py
   ```
   群内将在 1 秒内收到带 `@所有人` 的买入/清仓卡片通知，`state.json` 自动恢复为最新真实状态。
