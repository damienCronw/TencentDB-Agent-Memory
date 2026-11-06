#!/usr/bin/env python3
"""
scan_ports.py — 本机端口占用扫描器

用途：新项目选端口前先跑一遍，避免抢占已有服务。

用法:
  python3 scan_ports.py              # 扫描 + 打印
  python3 scan_ports.py --json       # 输出 JSON（供程序消费）
  python3 scan_ports.py --check 1234 # 只检查某个端口是否被占
  python3 scan_ports.py --free 9000  # 从 9000 起找 N 个连续空闲端口
"""
import subprocess
import re
import json
import sys
import os
from collections import defaultdict

# ── 已知项目归属（人工维护，新增服务时在此登记）────────────────────────
REGISTRY = {
    1883:  ('mosquitto',       'MQTT broker (homebrew)'),
    3283:  ('macOS',           'ARDAgent — 系统远程管理，勿动'),
    3906:  ('UlanziDeck',      'Ulanzi Studio 灯光控制台'),
    5432:  ('PostgreSQL 17',   '数据库 (homebrew)'),
    6379:  ('Redis',           'Redis — homebrew 已装但当前未运行'),
    8096:  ('TDAI',            'MemoryProxy — TDAI 记忆代理入口'),
    8123:  ('TDAI',            'MemoryPanel — TDAI Web 控制台'),
    8420:  ('TDAI',            'MemoryCore — TDAI 记忆核心（Hermes 插件直连）'),
    8421:  ('TDAI',            'MemoryKnowledge — TDAI 知识库'),
    11434: ('Ollama',          '本地 LLM 服务'),
    18789: ('(已释放)',         'OpenClaw gateway 老端口，已清理'),
    19333: ('OpenViking',      'OpenViking 记忆服务 (launchd)'),
}

# 建议为新项目预留的端口段（避免撞车）
RESERVED_RANGES = [
    (8400, 8499, 'TDAI 记忆系统'),
    (8100, 8199, 'TDAI Panel / Web UI'),
    (8000, 8099, 'TDAI Proxy / 通用后端'),
    (18700, 18999, 'AI Agent 网关'),
]


def scan():
    out = subprocess.run(['lsof', '-nP', '-iTCP', '-sTCP:LISTEN'],
                         capture_output=True, text=True).stdout
    rows, seen = [], set()
    for line in out.splitlines()[1:]:
        parts = line.split()
        if len(parts) < 9:
            continue
        proc, pid, _, _, _, _, _, _, addr = parts[:9]
        m = re.search(r':(\d+)$', addr)
        if not m:
            continue
        port = int(m.group(1))
        key = (port, pid)
        if key in seen:
            continue
        seen.add(key)
        cmd = subprocess.run(['ps', '-p', pid, '-o', 'command='],
                             capture_output=True, text=True).stdout.strip()
        rows.append({'port': port, 'pid': int(pid), 'proc': proc, 'cmd': cmd})
    rows.sort(key=lambda r: r['port'])
    return rows


def is_free(port):
    return not subprocess.run(
        f'lsof -i :{port} -sTCP:LISTEN -t',
        shell=True, capture_output=True, text=True).stdout.strip()


def main():
    args = sys.argv[1:]

    if '--check' in args:
        p = int(args[args.index('--check') + 1])
        print(('占用' if not is_free(p) else '空闲'), p)
        sys.exit(0 if is_free(p) else 1)

    if '--free' in args:
        start = int(args[args.index('--free') + 1])
        n = int(args[args.index('--free') + 2]) if len(args) > args.index('--free') + 2 else 3
        found = []
        p = start
        while len(found) < n and p < 65535:
            if is_free(p):
                found.append(p)
            p += 1
        print(found)
        sys.exit(0)

    rows = scan()
    if '--json' in args:
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return

    by_port = defaultdict(list)
    for r in rows:
        by_port[r['port']].append(r)

    print('=' * 100)
    print(f"本机 TCP LISTEN 端口占用  ({len(by_port)} 个端口 / {len(rows)} 个监听者)")
    print('=' * 100)
    print(f"{'端口':<8}{'PID':<9}{'进程':<11}{'归属':<16}命令")
    print('-' * 100)
    for port in sorted(by_port):
        for i, r in enumerate(by_port[port]):
            proj, desc = REGISTRY.get(port, ('—', '未登记'))
            pname = f"{proj}" if i == 0 else ''
            c = r['cmd'] if len(r['cmd']) <= 62 else r['cmd'][:59] + '...'
            print(f"{port if i == 0 else '':<8}{r['pid']:<9}{r['proc']:<11}{pname:<16}{c}")
    print()
    print('未登记端口（新增项目时注意）：')
    for port in sorted(by_port):
        if port not in REGISTRY:
            c = by_port[port][0]['cmd']
            print(f"  {port:<8} {by_port[port][0]['proc']:<12}{(c if len(c) <= 58 else c[:55] + '...')}")
    print()
    print('建议预留段：')
    for lo, hi, why in RESERVED_RANGES:
        used = [p for p in by_port if lo <= p <= hi]
        print(f"  {lo}-{hi}  {why}"
              f"{'  已占: ' + ','.join(map(str, used)) if used else '  当前全空 ✅'}")
    print()
    print('新项目选端口建议：用 --free <起始端口> 找空闲端口')


if __name__ == '__main__':
    main()
