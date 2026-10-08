"""用 10-07 真实数据按 _recv_ts 合并时间线回放新参数（4x + 单向 +15%）"""
import json
import logging
import sys

sys.path.insert(0, ".")
from strategy_engine import FundingArbStrategy, StrategyParams

# 参数从配置文件加载（不写死）：测试案例 = 1000U 保证金 / 4x / 名义 4000 / 现货 4000
params = StrategyParams.from_file("config/strategy_test.json")
print(f"参数(配置): 杠杆={params.leverage}x 保证金={params.collateral_usd} 名义={params.perp_notional} "
      f"现货={params.spot_usd} 触发=+{params.deviation_trigger*100:.0f}%")

s = FundingArbStrategy(params)

TARGET = "10"  # MON (mainnet market id 10)
# 读 funding 帧，记录 (recv_ts, rate) 序列
funding_events = []  # [(recv_ts, rate)]
for line in open("data/2026-10-07/funding.jsonl", encoding="utf-8"):
    fr = json.loads(line)
    d = fr.get("d") or {}
    if TARGET in d and "rate" in d[TARGET]:
        funding_events.append((fr.get("_recv_ts", 0), d[TARGET]["rate"]))
funding_events.sort()

# 读 market_state 帧
mstate = []
for line in open("data/2026-10-07/market_state.jsonl", encoding="utf-8"):
    fr = json.loads(line)
    d = fr.get("d") or {}
    if TARGET in d:
        mstate.append(fr)
mstate.sort(key=lambda x: x.get("_recv_ts", 0))

print(f"funding 事件: {len(funding_events)}, market_state 帧: {len(mstate)}")

# 合并时间线：维护当前 funding rate，funding 帧到达时标记一次结算事件
fi = 0
cur_rate = 0.0
last_fund_ts = -1
events = 0
for fr in mstate:
    rts = fr.get("_recv_ts", 0)
    # 处理所有已到达的 funding 帧
    while fi < len(funding_events) and funding_events[fi][0] <= rts:
        cur_rate = funding_events[fi][1] / 1e6   # scaled → 小数 (40 → 0.00004)
        fi += 1
    st = fr["d"][TARGET]
    mrk = st.get("mrk") or 0
    orl = st.get("orl") or 0
    ts = fr.get("at", {}).get("t", 0)
    if not (mrk and orl):
        continue
    # funding 结算事件判定：cur_rate 刚更新且距上次事件 >30min
    is_fund = False
    if fi > 0 and funding_events[fi-1][0] == rts:
        pass  # 同帧到达不算
    # 简化：funding_events 的时间戳就是结算时点，用最近一次 funding 的 at.t 判定
    # 改用 funding 帧数量判定：fi 增加时标记
    # 这里用 funding_events[fi-1] 作为"当前已生效的 funding"，判断是否是新结算
    if fi > 0 and funding_events[fi-1][0] != last_fund_ts:
        last_fund_ts = funding_events[fi-1][0]
        is_fund = True
    dec = s.update_market(TARGET, cur_rate, orl, mrk, ts, is_funding_event=is_fund)
    if dec:
        print(f"  [{ts}] {dec.action}: {dec.reason}")
        events += 1

print(f"\n决策数: {events}, 状态: {s.state}")
if s.pos:
    print(f"  perp名义={s.pos.perp_size} spot={s.pos.spot_size} "
          f"累计funding={s.pos.funding_collected:.4f} 事件={s.pos.funding_events}")
print("✅ 回放完成")
