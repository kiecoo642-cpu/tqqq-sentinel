import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch
import numpy as np
import pandas as pd

import main as sentinel


def make_mock_df(prices, start_date="2025-01-01", tz="America/New_York", multiindex=False, col_name="Close"):
    """构造测试用历史日线 DataFrame"""
    dates = pd.bdate_range(start=start_date, periods=len(prices), tz=tz)
    if multiindex:
        cols = pd.MultiIndex.from_tuples([(col_name, "QQQ")])
        df = pd.DataFrame(np.array(prices, dtype=float).reshape(-1, 1), index=dates, columns=cols)
    else:
        df = pd.DataFrame({col_name: np.array(prices, dtype=float)}, index=dates)
    return df


class TestTQQQSentinel(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state_file = os.path.join(self.temp_dir.name, "state.json")
        self.state_patcher = patch.object(sentinel, "STATE_FILE", self.state_file)
        self.state_patcher.start()
        self.snap_patcher = patch.object(sentinel, "get_market_snapshot", return_value={})
        self.snap_patcher.start()
        self.env_patcher = patch.dict(os.environ, {}, clear=False)
        self.env_patcher.start()
        for k in (
            "DAILY_BRIEF",
            "WEBHOOK_URL",
            "WEBHOOK_SECRET",
            "FEISHU_WEBHOOK",
            "FEISHU_SECRET",
            "WEWORK_WEBHOOK",
            "DINGTALK_WEBHOOK",
            "DINGTALK_SECRET",
            "PUSHPLUS_TOKEN",
        ):
            os.environ.pop(k, None)

    def tearDown(self):
        self.env_patcher.stop()
        self.snap_patcher.stop()
        self.state_patcher.stop()
        self.temp_dir.cleanup()

    @patch.object(sentinel, "send_notification")
    @patch.object(sentinel, "get_qqq_data_with_retry")
    def test_01_first_run_initialization(self, mock_data, mock_notify):
        """测试 01：首次运行无 state.json 时静默初始化，不发通知"""
        mock_data.return_value = ("2026-10-09", 750.00, 670.00)
        with redirect_stdout(io.StringIO()):
            sentinel.main()

        mock_notify.assert_not_called()
        self.assertTrue(os.path.exists(self.state_file))
        with open(self.state_file, "r", encoding="utf-8") as f:
            saved = json.load(f)
        self.assertEqual(saved["last_state"], "LONG")
        self.assertEqual(saved["last_date"], "2026-10-09")
        self.assertEqual(saved["schema_version"], 1)

    @patch.object(sentinel, "send_notification", return_value=True)
    @patch.object(sentinel, "get_qqq_data_with_retry")
    def test_02_cash_to_long_flip_triggers_buy_alert(self, mock_data, mock_notify):
        """测试 02：CASH -> LONG 状态翻转触发买入信号并更新持久化状态"""
        sentinel.save_current_state({"schema_version": 1, "last_state": "CASH", "last_date": "2026-10-08"})
        mock_data.return_value = ("2026-10-09", 680.00, 670.00)

        with redirect_stdout(io.StringIO()):
            sentinel.main()

        mock_notify.assert_called_once()
        title, content = mock_notify.call_args[0]
        self.assertIn("入场指令", title)
        self.assertIn("+1.49%", content)
        self.assertIn("$680.00", content)

        saved = sentinel.load_previous_state()
        self.assertEqual(saved["last_state"], "LONG")
        self.assertEqual(saved["last_date"], "2026-10-09")

    @patch.object(sentinel, "send_notification", return_value=True)
    @patch.object(sentinel, "get_qqq_data_with_retry")
    def test_03_long_to_cash_flip_triggers_sell_alert(self, mock_data, mock_notify):
        """测试 03：LONG -> CASH 状态翻转触发清仓避险信号并带负号偏离度"""
        sentinel.save_current_state({"schema_version": 1, "last_state": "LONG", "last_date": "2026-10-08"})
        mock_data.return_value = ("2026-10-09", 650.00, 670.00)

        with redirect_stdout(io.StringIO()):
            sentinel.main()

        mock_notify.assert_called_once()
        title, content = mock_notify.call_args[0]
        self.assertIn("离场指令", title)
        self.assertIn("-2.99%", content)
        self.assertIn("清仓 TQQQ", content)

        saved = sentinel.load_previous_state()
        self.assertEqual(saved["last_state"], "CASH")
        self.assertEqual(saved["last_date"], "2026-10-09")

    @patch.object(sentinel, "send_notification", return_value=True)
    @patch.object(sentinel, "get_qqq_data_with_retry")
    def test_04_exact_equality_boundary_is_long(self, mock_data, mock_notify):
        """测试 04：边界值测试（Close == SMA200 严格判定为 LONG）"""
        sentinel.save_current_state({"schema_version": 1, "last_state": "CASH", "last_date": "2026-10-08"})
        mock_data.return_value = ("2026-10-09", 670.00, 670.00)

        with redirect_stdout(io.StringIO()):
            sentinel.main()

        mock_notify.assert_called_once()
        title, content = mock_notify.call_args[0]
        self.assertIn("入场指令", title)
        self.assertIn("+0.00%", content)
        self.assertEqual(sentinel.load_previous_state()["last_state"], "LONG")

    @patch.object(sentinel, "send_notification")
    @patch.object(sentinel, "get_qqq_data_with_retry")
    def test_05_no_state_change_silent_exit(self, mock_data, mock_notify):
        """测试 05：新交易日状态未变时保持静默，不触发推送且不重写 state.json"""
        sentinel.save_current_state({"schema_version": 1, "last_state": "LONG", "last_date": "2026-10-08"})
        mock_data.return_value = ("2026-10-09", 755.00, 671.00)

        with redirect_stdout(io.StringIO()):
            sentinel.main()

        mock_notify.assert_not_called()
        self.assertEqual(sentinel.load_previous_state()["last_date"], "2026-10-08")

    @patch.object(sentinel, "send_notification")
    @patch.object(sentinel, "get_qqq_data_with_retry")
    def test_06_same_trade_date_holiday_or_duplicate_run_guard(self, mock_data, mock_notify):
        """测试 06：美股节假日休市或同日重复执行时直接静默拦截"""
        sentinel.save_current_state({"schema_version": 1, "last_state": "LONG", "last_date": "2026-10-09"})
        mock_data.return_value = ("2026-10-09", 751.27, 670.88)

        buf = io.StringIO()
        with redirect_stdout(buf):
            sentinel.main()

        mock_notify.assert_not_called()
        self.assertIn("已处理且状态未变", buf.getvalue())

    @patch.object(sentinel, "send_notification", return_value=False)
    @patch.object(sentinel, "get_qqq_data_with_retry")
    def test_07_notification_failure_prevents_state_save_and_exits_nonzero(self, mock_data, mock_notify):
        """测试 07：当推送通知失败时，严禁更新 state.json 并以非零状态码退出以保护信号不丢失"""
        sentinel.save_current_state({"schema_version": 1, "last_state": "LONG", "last_date": "2026-10-08"})
        mock_data.return_value = ("2026-10-09", 640.00, 670.00)

        with redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as cm:
                sentinel.main()

        self.assertIn("推送通知失败", str(cm.exception))
        self.assertEqual(sentinel.load_previous_state()["last_state"], "LONG")

    def test_08_dirty_and_bom_state_json_handling(self):
        """测试 08：脏数据、大小写、损坏 JSON 与 Windows UTF-8 BOM 兼容性"""
        with open(self.state_file, "wb") as f:
            f.write(b"\xef\xbb\xbf" + json.dumps({"last_state": "  cash \n"}).encode("utf-8"))
        self.assertEqual(sentinel.load_previous_state()["last_state"], "CASH")

        sentinel.save_current_state({"last_state": "INVALID_STATE"})
        with redirect_stdout(io.StringIO()):
            self.assertEqual(sentinel.load_previous_state()["last_state"], "UNKNOWN")

        with open(self.state_file, "w", encoding="utf-8") as f:
            f.write("{corrupted json")
        with redirect_stdout(io.StringIO()):
            self.assertEqual(sentinel.load_previous_state()["last_state"], "UNKNOWN")

        with open(self.state_file, "w", encoding="utf-8") as f:
            f.write("[1, 2, 3]")
        with redirect_stdout(io.StringIO()):
            self.assertEqual(sentinel.load_previous_state()["last_state"], "UNKNOWN")

    @patch("yfinance.Ticker")
    def test_09_multiindex_and_whitespace_columns_compatibility(self, mock_ticker_cls):
        """测试 09：兼容 yfinance MultiIndex 列名及带空格列名"""
        prices = [500.0] * 199 + [700.0]
        df = make_mock_df(prices, start_date="2025-01-01", multiindex=True, col_name=" Close ")
        mock_ticker_cls.return_value.history.return_value = df

        date_str, close_price, sma_200 = sentinel.get_qqq_data_with_retry(retries=1, delay=0)
        self.assertEqual(close_price, 700.00)
        self.assertEqual(sma_200, 501.00)
        self.assertRegex(date_str, r"^\d{4}-\d{2}-\d{2}$")

    @patch("yfinance.Ticker")
    def test_10_insufficient_rows_and_retry_recovery(self, mock_ticker_cls):
        """测试 10：网络波动及数据不足 200 行时的重试恢复与最终退出机制"""
        df_short = make_mock_df([500.0] * 150)
        df_valid = make_mock_df([600.0] * 220)

        mock_ticker_cls.return_value.history.side_effect = [
            RuntimeError("Connection reset"),
            df_short,
            df_valid,
        ]
        with redirect_stdout(io.StringIO()):
            _, close_price, sma_200 = sentinel.get_qqq_data_with_retry(retries=3, delay=0)
        self.assertEqual(close_price, 600.00)
        self.assertEqual(sma_200, 600.00)

        mock_ticker_cls.return_value.history.side_effect = RuntimeError("API Down")
        with redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                sentinel.get_qqq_data_with_retry(retries=2, delay=0)

    @patch("yfinance.Ticker")
    def test_11_intraday_unclosed_bar_filtering(self, mock_ticker_cls):
        """测试 11：盘中未收盘 K 线自动剔除，防止盘中手动运行产生假突破信号"""
        prices = [600.0] * 204 + [400.0]
        df = make_mock_df(prices, start_date="2025-01-01", tz="America/New_York")
        last_bar_date = df.index[-1]
        mock_ticker_cls.return_value.history.return_value = df

        intraday_now = pd.Timestamp(
            year=last_bar_date.year,
            month=last_bar_date.month,
            day=last_bar_date.day,
            hour=11,
            minute=0,
            tz="America/New_York",
        )
        with patch.object(pd.Timestamp, "now", return_value=intraday_now):
            buf = io.StringIO()
            with redirect_stdout(buf):
                date_str, close_price, _ = sentinel.get_qqq_data_with_retry(retries=1, delay=0)

        self.assertIn("剔除", buf.getvalue())
        self.assertEqual(close_price, 600.00)
        self.assertEqual(date_str, df.index[-2].strftime("%Y-%m-%d"))

        after_close_now = pd.Timestamp(
            year=last_bar_date.year,
            month=last_bar_date.month,
            day=last_bar_date.day,
            hour=18,
            minute=30,
            tz="America/New_York",
        )
        with patch.object(pd.Timestamp, "now", return_value=after_close_now):
            date_str2, close_price2, _ = sentinel.get_qqq_data_with_retry(retries=1, delay=0)

        self.assertEqual(close_price2, 400.00)
        self.assertEqual(date_str2, df.index[-1].strftime("%Y-%m-%d"))

    @patch("requests.post")
    def test_12_feishu_webhook_interactive_card_and_signature(self, mock_post):
        """测试 12：飞书 (Feishu) 群机器人彩色卡片格式、@所有人 与 HMAC-SHA256 签名校验"""
        feishu_url = "https://open.feishu.cn/open-apis/bot/v2/hook/xxxx-yyyy"
        resp_ok = MagicMock()
        resp_ok.status_code = 200
        resp_ok.text = '{"code": 0, "msg": "success"}'
        resp_ok.json.return_value = {"code": 0, "msg": "success"}
        mock_post.return_value = resp_ok

        with patch.dict(
            os.environ,
            {"WEBHOOK_URL": feishu_url, "WEBHOOK_SECRET": "feishu_sec_123"},
        ):
            with redirect_stdout(io.StringIO()):
                ok = sentinel.send_notification("🚨【入场指令】突破", "内容详情")

        self.assertTrue(ok)
        sent_url = mock_post.call_args[0][0]
        sent_json = mock_post.call_args[1]["json"]
        self.assertEqual(sent_url, feishu_url)
        self.assertEqual(sent_json["msg_type"], "interactive")
        self.assertEqual(sent_json["card"]["header"]["template"], "red")
        self.assertIn("<at id=all></at>", sent_json["card"]["elements"][0]["content"])
        self.assertIn("sign", sent_json)
        self.assertIn("timestamp", sent_json)

    @patch("requests.post")
    def test_13_wework_webhook_markdown_and_errcode(self, mock_post):
        """测试 13：企业微信 (WeCom) 群机器人 Markdown 格式、@all 与 errcode 校验"""
        wecom_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abcd-1234"
        resp_ok = MagicMock()
        resp_ok.status_code = 200
        resp_ok.text = '{"errcode": 0, "errmsg": "ok"}'
        resp_ok.json.return_value = {"errcode": 0, "errmsg": "ok"}
        mock_post.return_value = resp_ok

        with patch.dict(os.environ, {"WEBHOOK_URL": wecom_url}):
            with redirect_stdout(io.StringIO()):
                ok = sentinel.send_notification("⚠️【离场指令】清仓", "内容详情")

        self.assertTrue(ok)
        sent_json = mock_post.call_args[1]["json"]
        self.assertEqual(sent_json["msgtype"], "markdown")
        self.assertIn("<@all>", sent_json["markdown"]["content"])

        # 企业微信返回 errcode != 0 (如 key 错误 93000) 时应判定失败
        resp_err = MagicMock()
        resp_err.status_code = 200
        resp_err.text = '{"errcode": 93000, "errmsg": "invalid webhook url"}'
        resp_err.json.return_value = {"errcode": 93000, "errmsg": "invalid webhook url"}
        mock_post.side_effect = [resp_err, resp_err, resp_err]
        with patch.dict(os.environ, {"WEBHOOK_URL": wecom_url}):
            with patch("time.sleep"), redirect_stdout(io.StringIO()):
                self.assertFalse(sentinel.send_notification("标题", "内容"))

    @patch("requests.post")
    def test_14_dingtalk_webhook_signed_url_and_markdown(self, mock_post):
        """测试 14：钉钉 (DingTalk) 群机器人 URL 加签参数拼接、isAtAll 与 Markdown 校验"""
        ding_url = "https://oapi.dingtalk.com/robot/send?access_token=token123"
        resp_ok = MagicMock()
        resp_ok.status_code = 200
        resp_ok.text = '{"errcode": 0, "errmsg": "ok"}'
        resp_ok.json.return_value = {"errcode": 0, "errmsg": "ok"}
        mock_post.return_value = resp_ok

        with patch.dict(
            os.environ,
            {"WEBHOOK_URL": ding_url, "WEBHOOK_SECRET": "SECtestsecret123"},
        ):
            with redirect_stdout(io.StringIO()):
                ok = sentinel.send_notification("🚨【入场指令】", "内容详情")

        self.assertTrue(ok)
        called_url = mock_post.call_args[0][0]
        sent_json = mock_post.call_args[1]["json"]
        self.assertIn("&timestamp=", called_url)
        self.assertIn("&sign=", called_url)
        self.assertEqual(sent_json["msgtype"], "markdown")
        self.assertTrue(sent_json["at"]["isAtAll"])

    @patch("requests.post")
    def test_15_multi_channel_concurrent_webhook_redundancy(self, mock_post):
        """测试 15：多群机器人双通道冗余（一个通道故障、另一个通道成功时整体仍可靠触达）"""
        feishu_url = "https://open.feishu.cn/open-apis/bot/v2/hook/111"
        wecom_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=222"

        resp_fail = MagicMock()
        resp_fail.status_code = 500
        resp_fail.raise_for_status.side_effect = sentinel.requests.exceptions.HTTPError("500")

        resp_ok = MagicMock()
        resp_ok.status_code = 200
        resp_ok.text = '{"errcode": 0, "errmsg": "ok"}'
        resp_ok.json.return_value = {"errcode": 0, "errmsg": "ok"}

        # 飞书连续 3 次 500 失败，企业微信第 1 次成功
        mock_post.side_effect = [resp_fail, resp_fail, resp_fail, resp_ok]

        with patch.dict(os.environ, {"WEBHOOK_URL": f"{feishu_url}, {wecom_url}"}):
            with patch("time.sleep"), redirect_stdout(io.StringIO()):
                ok = sentinel.send_notification("标题", "内容")

        self.assertTrue(ok)
        self.assertEqual(mock_post.call_count, 4)

    @patch("requests.post")
    def test_16_pushplus_fallback_and_non_json_gateway_error(self, mock_post):
        """测试 16：PushPlus 兼容通道、Token 引号自动清洗 + 502 HTML 非 JSON 响应容错"""
        with patch.dict(os.environ, {"PUSHPLUS_TOKEN": '  "my_secret_token"  '}):
            resp_ok = MagicMock()
            resp_ok.status_code = 200
            resp_ok.text = '{"code": 200}'
            resp_ok.json.return_value = {"code": 200}
            mock_post.return_value = resp_ok

            with redirect_stdout(io.StringIO()):
                sentinel.send_notification("标题", "内容")

            sent_payload = mock_post.call_args[1]["json"]
            self.assertEqual(sent_payload["token"], "my_secret_token")

            resp_html = MagicMock()
            resp_html.status_code = 200
            resp_html.text = "<html>502 Bad Gateway</html>"
            resp_html.json.side_effect = ValueError("No JSON object could be decoded")
            mock_post.side_effect = [resp_html, resp_html, resp_html]

            with patch("time.sleep"), redirect_stdout(io.StringIO()):
                self.assertFalse(sentinel.send_notification("标题", "内容"))

    @patch("yfinance.Ticker")
    def test_17_duplicate_index_unsorted_and_nan_cleaning(self, mock_ticker_cls):
        """测试 17：数据源乱序、重复日期索引与 NaN 空值行自动清洗"""
        prices = [500.0] * 100 + [np.nan] + [500.0] * 99 + [700.0]
        df = make_mock_df(prices, start_date="2025-01-01")
        dup_row = pd.DataFrame({"Close": [100.0]}, index=[df.index[-1]])
        df_dirty = pd.concat([df.iloc[50:], dup_row, df.iloc[:50]])
        df_dirty = pd.concat([df_dirty, pd.DataFrame({"Close": [700.0]}, index=[df.index[-1]])])
        mock_ticker_cls.return_value.history.return_value = df_dirty

        _, close_price, sma_200 = sentinel.get_qqq_data_with_retry(retries=1, delay=0)
        self.assertEqual(close_price, 700.00)
        self.assertEqual(sma_200, 501.00)

    @patch("yfinance.Ticker")
    def test_18_zero_or_negative_corrupt_price_rejection(self, mock_ticker_cls):
        """测试 18：数据源返回 0 或负数异常价格时主动拦截并触发重试"""
        df_zero = make_mock_df([500.0] * 199 + [0.0], start_date="2025-01-01")
        df_neg = make_mock_df([-500.0] * 200, start_date="2025-01-01")
        mock_ticker_cls.return_value.history.side_effect = [df_zero, df_neg]

        with redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit):
                sentinel.get_qqq_data_with_retry(retries=2, delay=0)

    def test_19_github_workflow_yaml_audit(self):
        """测试 19：静态审计 .github/workflows/monitor.yml 关键安全与 Webhook 环境变量注入"""
        workflow_path = os.path.join(
            os.path.dirname(os.path.abspath(sentinel.__file__)),
            ".github",
            "workflows",
            "monitor.yml",
        )
        self.assertTrue(os.path.exists(workflow_path))
        with open(workflow_path, "r", encoding="utf-8") as f:
            yml_text = f.read()

        self.assertIn("cron: '0 2 * * 2-6'", yml_text)
        self.assertIn('DAILY_BRIEF: "1"', yml_text)
        self.assertIn("contents: write", yml_text)
        self.assertIn("timeout-minutes: 10", yml_text)
        self.assertIn("if: success()", yml_text)
        self.assertIn("WEBHOOK_URL: ${{ secrets.WEBHOOK_URL }}", yml_text)
        self.assertIn("WEBHOOK_SECRET: ${{ secrets.WEBHOOK_SECRET }}", yml_text)
        self.assertIn("[skip ci]", yml_text)

    def test_20_live_qqq_historical_backtest_simulation(self):
        """测试 20：真实 QQQ 2 年历史日线逐日回放压力测试"""
        ticker = sentinel.yf.Ticker("QQQ")
        df = ticker.history(period="2y", interval="1d", auto_adjust=True)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.dropna(subset=["Close"]).sort_index()
        df = df[~df.index.duplicated(keep="last")].copy()
        self.assertGreaterEqual(len(df), 250)

        df["SMA_200"] = df["Close"].rolling(window=200).mean()
        valid_df = df.dropna(subset=["SMA_200"])

        flips = []
        last_state = None
        for idx, row in valid_df.iterrows():
            c = round(float(row["Close"]), 2)
            m = round(float(row["SMA_200"]), 2)
            state = "LONG" if c >= m else "CASH"
            d_str = idx.tz_convert("America/New_York").strftime("%Y-%m-%d") if idx.tzinfo else idx.strftime("%Y-%m-%d")
            if last_state is not None and state != last_state:
                flips.append((d_str, last_state, state, c, m))
            last_state = state

        print(f"\n[真实数据回测] 有效回放交易日: {len(valid_df)} 天 | 触发状态翻转次数: {len(flips)} 次")
        for d_str, s_from, s_to, c, m in flips:
            print(f"  - {d_str}: {s_from} -> {s_to} (QQQ收盘: ${c:.2f}, 200MA: ${m:.2f})")


if __name__ == "__main__":
    unittest.main(verbosity=2)
