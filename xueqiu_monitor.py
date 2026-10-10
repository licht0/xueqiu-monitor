#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xueqiu_monitor.py
近实时监控雪球用户发言（默认：段永平 / 大道无形我有型, uid=1247347556）

功能：
- 轮询雪球用户时间线接口，抓取本人发言以及引用/转发的原帖内容(retweeted_status)
- SQLite 持久化去重 + JSONL 增量导出，互动数(评论/转发/赞)自动更新
- 内置网页面板：启动后浏览器打开 http://127.0.0.1:8765 即可浏览，不依赖终端
- 阿里云 WAF(JS 挑战)处理：优先使用本地 cookie；失效时可用 Playwright 无头浏览器自动引导
- 主体仅依赖 Python 标准库；全自动引导需要可选依赖 playwright

用法示例：
  python3 xueqiu_monitor.py                         # 默认监控段永平，每小时轮询，浏览器看面板
  python3 xueqiu_monitor.py --interval 30           # 30 秒轮询一次
  python3 xueqiu_monitor.py --once                  # 抓取一次后停止轮询（网页面板仍运行）
  python3 xueqiu_monitor.py --backfill 5            # 首次启动回填最近 5 页历史
  python3 xueqiu_monitor.py --no-web                # 不启动网页面板，仅终端输出
  python3 xueqiu_monitor.py --cookie "xq_a_token=..; ssxmod_itna=.."  # 手动指定 cookie
  python3 xueqiu_monitor.py --user-id 1247347556    # 监控其他用户
"""

import argparse
import base64
import hashlib
import hmac
import html as html_mod
import json
import os
import random
import sqlite3
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.cookiejar import Cookie, CookieJar
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from html.parser import HTMLParser
from urllib.parse import parse_qs, urlparse

# --------------------------------------------------------------------------- #
# 常量
# --------------------------------------------------------------------------- #

DEFAULT_USER_ID = 1247347556  # 段永平：大道无形我有型
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")
BASE_URL = "https://www.xueqiu.com"
TIMELINE_API = BASE_URL + "/statuses/user_timeline.json"
PAGE_SIZE = 20


# --------------------------------------------------------------------------- #
# 工具函数
# --------------------------------------------------------------------------- #

def log(msg):
    """带时间戳的日志输出"""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print("[%s] %s" % (ts, msg), flush=True)


def ts_to_str(ms):
    """毫秒时间戳 -> 北京时间字符串（固定 UTC+8，不依赖运行环境时区）"""
    if not ms:
        return ""
    from datetime import datetime, timezone, timedelta
    bj = timezone(timedelta(hours=8))
    return datetime.fromtimestamp(ms / 1000.0, tz=bj).strftime("%Y-%m-%d %H:%M:%S")


def decode_jwt_payload(token):
    """解析 JWT 中段 payload；失败时返回 None。"""
    try:
        payload_part = token.split(".")[1]
        padded = payload_part + "=" * (-len(payload_part) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        data = json.loads(raw.decode("utf-8"))
    except (IndexError, ValueError, UnicodeError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


class _PlainTextExtractor(HTMLParser):
    """把雪球 description 的 HTML 转成可读纯文本"""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []

    def handle_starttag(self, tag, attrs):
        attrs_d = dict(attrs)
        if tag == "br":
            self.parts.append("\n")
        elif tag == "img":
            title = attrs_d.get("title") or ""
            src = attrs_d.get("src") or ""
            if title:
                # 表情图，title 形如 [很赞]
                self.parts.append(title)
            elif "emoji" in src:
                pass
            else:
                self.parts.append("[图片]")
        elif tag in ("p", "div"):
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("p", "div"):
            self.parts.append("\n")

    def handle_data(self, data):
        self.parts.append(data)

    def get_text(self):
        text = "".join(self.parts)
        # 规整多余空白（保留换行）
        lines = [ln.strip() for ln in text.split("\n")]
        text = "\n".join(lines)
        while "\n\n\n" in text:
            text = text.replace("\n\n\n", "\n\n")
        return text.strip()


def html_to_text(raw_html):
    if not raw_html:
        return ""
    parser = _PlainTextExtractor()
    try:
        parser.feed(raw_html)
    except Exception:
        return html_mod.unescape(raw_html)
    return parser.get_text()


# --------------------------------------------------------------------------- #
# 存储
# --------------------------------------------------------------------------- #

class Storage:
    """SQLite 存储 + JSONL 导出"""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS posts (
        id             INTEGER PRIMARY KEY,
        user_id        INTEGER,
        created_at     INTEGER,
        source         TEXT,
        title          TEXT,
        text           TEXT,
        raw_html       TEXT,
        pic            TEXT,
        has_retweet    INTEGER,
        rt_id          INTEGER,
        rt_user_id     INTEGER,
        rt_user        TEXT,
        rt_created_at  INTEGER,
        rt_text        TEXT,
        rt_raw_html    TEXT,
        rt_pic         TEXT,
        reply_count    INTEGER,
        retweet_count  INTEGER,
        like_count     INTEGER,
        url            TEXT,
        first_seen     INTEGER,
        updated_at     INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at);
    """

    _MIGRATIONS = (
        "ALTER TABLE posts ADD COLUMN pic TEXT",
        "ALTER TABLE posts ADD COLUMN rt_pic TEXT",
    )

    def __init__(self, db_path, jsonl_path):
        self.db_path = db_path
        self.jsonl_path = jsonl_path
        self.conn = sqlite3.connect(db_path)
        self.conn.executescript(self.SCHEMA)
        # 旧库迁移：新增字段
        for stmt in self._MIGRATIONS:
            try:
                self.conn.execute(stmt)
            except sqlite3.OperationalError:
                pass
        self.conn.commit()

    def exists(self, post_id):
        cur = self.conn.execute("SELECT 1 FROM posts WHERE id=?", (post_id,))
        return cur.fetchone() is not None

    def upsert(self, rec):
        """
        写入或更新帖子。
        返回 True 表示新帖，False 表示已存在（仅更新互动数）。
        """
        now = int(time.time() * 1000)
        is_new = not self.exists(rec["id"])
        pic = rec.get("pic", "") or ""
        rt_pic = rec.get("rt_pic", "") or ""
        if is_new:
            self.conn.execute("""
                INSERT INTO posts (id, user_id, created_at, source, title, text, raw_html,
                                   pic, has_retweet, rt_id, rt_user_id, rt_user, rt_created_at,
                                   rt_text, rt_raw_html, rt_pic, reply_count, retweet_count,
                                   like_count, url, first_seen, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                rec["id"], rec["user_id"], rec["created_at"], rec["source"],
                rec["title"], rec["text"], rec["raw_html"], pic,
                1 if rec["has_retweet"] else 0,
                rec["rt_id"], rec["rt_user_id"], rec["rt_user"], rec["rt_created_at"],
                rec["rt_text"], rec["rt_raw_html"], rt_pic,
                rec["reply_count"], rec["retweet_count"], rec["like_count"],
                rec["url"], now, now,
            ))
        else:
            # 互动数每次更新；引用内容/图片只在“原先缺失、本次抓到”时补存，
            # 已保存的内容永不覆盖（防止原帖删除/修改后丢失快照）。
            # 例外：若已存引用内容被截断（末尾 ...），则用本次完整内容替换。
            row = self.conn.execute(
                "SELECT COALESCE(text,''), COALESCE(rt_text,''), COALESCE(rt_raw_html,''), "
                "COALESCE(rt_pic,''), COALESCE(pic,'') FROM posts WHERE id=?",
                (rec["id"],)).fetchone()
            stored_text, stored_rt, stored_rt_html, stored_rt_pic, stored_pic = row
            text_truncated = stored_text.rstrip().endswith("...") and len(stored_text) > 50
            rt_truncated = stored_rt.rstrip().endswith("...") and len(stored_rt) > 50
            updates = ["reply_count=?", "retweet_count=?", "like_count=?", "updated_at=?"]
            vals = [rec["reply_count"], rec["retweet_count"], rec["like_count"], now]
            if text_truncated and rec["text"]:
                updates += ["text=?", "raw_html=?"]
                vals += [rec["text"], rec["raw_html"]]
            if rec["has_retweet"]:
                if not stored_rt or rt_truncated:
                    updates += ["has_retweet=1", "rt_id=?", "rt_user_id=?", "rt_user=?",
                                "rt_created_at=?", "rt_text=?", "rt_raw_html=?"]
                    vals += [rec["rt_id"], rec["rt_user_id"], rec["rt_user"],
                             rec["rt_created_at"], rec["rt_text"], rec["rt_raw_html"]]
            if rt_pic and not stored_rt_pic:
                updates.append("rt_pic=?")
                vals.append(rt_pic)
            if pic and not stored_pic:
                updates.append("pic=?")
                vals.append(pic)
            vals.append(rec["id"])
            self.conn.execute(
                "UPDATE posts SET %s WHERE id=?" % ", ".join(updates), vals)
        self.conn.commit()
        return is_new

    def append_jsonl(self, rec):
        with open(self.jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def count(self):
        cur = self.conn.execute("SELECT COUNT(*) FROM posts")
        return cur.fetchone()[0]

    def close(self):
        self.conn.close()


# --------------------------------------------------------------------------- #
# 网页展示面板（标准库实现，无额外依赖）
# --------------------------------------------------------------------------- #

# 不暴露给网页的字段（原始 HTML 体积大且页面不需要）
_WEB_HIDDEN_COLS = ("first_seen", "updated_at")


def query_posts(db_path, limit=20, before_id=None):
    """从 SQLite 查询帖子，返回 (帖子dict列表, 总数)。每次请求独立连接，线程安全。"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if before_id is not None:
            rows = conn.execute(
                "SELECT * FROM posts WHERE id < ? "
                "ORDER BY created_at DESC, id DESC LIMIT ?",
                (before_id, limit)).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM posts ORDER BY created_at DESC, id DESC LIMIT ?",
                (limit,)).fetchall()
        total = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
    finally:
        conn.close()

    posts = []
    for r in rows:
        d = dict(r)
        for col in _WEB_HIDDEN_COLS:
            d.pop(col, None)
        d["time_str"] = ts_to_str(d.get("created_at"))
        d["rt_time_str"] = ts_to_str(d.get("rt_created_at"))
        posts.append(d)
    return posts, total


def query_all_posts(db_path):
    """读取全部帖子（用于静态快照内嵌），按时间倒序"""
    posts, _ = query_posts(db_path, limit=10**9)
    return posts


# 口令遮罩层：页面数据已在生成时加密，解密完全在浏览器本地完成。
_GATE_HTML = """
<div id="gate">
  <form id="gateForm">
    <div class="gateTitle">请输入访问口令</div>
    <input id="gateInput" type="password" autocomplete="off" placeholder="口令">
    <button id="gateSubmit" type="submit">进入</button>
    <div id="gateErr"></div>
  </form>
</div>
<style>
#gate{position:fixed;inset:0;z-index:9999;background:#0f8a6c;
 display:flex;align-items:center;justify-content:center;font-family:-apple-system,
 "PingFang SC","Microsoft YaHei",sans-serif}
#gateForm{background:#fff;border-radius:14px;padding:30px 28px;width:280px;text-align:center}
.gateTitle{font-size:16px;font-weight:600;margin-bottom:16px;color:#1f2329}
#gateInput{width:100%;padding:10px 12px;border:1px solid #dcdfe3;border-radius:8px;
 font-size:15px;outline:none}
#gateInput:focus{border-color:#0f8a6c}
#gateForm button{width:100%;margin-top:12px;padding:10px;border:0;border-radius:8px;
 background:#0f8a6c;color:#fff;font-size:15px;cursor:pointer}
#gateForm button:disabled{opacity:.65;cursor:wait}
#gateErr{color:#d83931;font-size:13px;margin-top:10px;min-height:18px}
</style>
<script>
(function(){
  var KEY='xq_snapshot_pwd';
  function tryDecrypt(pwd){
    if(!window.__xqDecrypt) return false;
    window.__xqDecrypt(pwd).then(function(){
      try{ localStorage.setItem(KEY,pwd); }catch(e){}
    }).catch(function(){
      try{ localStorage.removeItem(KEY); }catch(e){}
    });
    return true;
  }
  function getFromUrl(){
    var m=location.hash.match(/[#&]pwd=([^&]+)/);
    return m ? decodeURIComponent(m[1]) : '';
  }
  document.addEventListener('DOMContentLoaded',function(){
    var form=document.getElementById('gateForm');
    var input=document.getElementById('gateInput');
    var button=document.getElementById('gateSubmit');
    var err=document.getElementById('gateErr');
    input.focus();
    // 优先用 URL 参数，其次用 localStorage 记忆
    var urlPwd=getFromUrl();
    if(urlPwd){ input.value=urlPwd; tryDecrypt(urlPwd); }
    else {
      try{
        var saved=localStorage.getItem(KEY);
        if(saved){ input.value=saved; tryDecrypt(saved); }
      }catch(e){}
    }
    form.addEventListener('submit',function(ev){
      ev.preventDefault();
      err.textContent='';
      if(!window.__xqDecrypt){ err.textContent='页面尚未准备好'; return; }
      button.disabled=true;
      window.__xqDecrypt(input.value)
        .then(function(){ try{ localStorage.setItem(KEY,input.value); }catch(e){} })
        .catch(function(){ err.textContent='口令不正确或数据无法解密'; input.select(); })
        .then(function(){ button.disabled=false; });
    });
  });
})();
</script>
"""


def b64e(raw):
    return base64.b64encode(raw).decode("ascii")


def encrypt_snapshot_payload(payload, password):
    """用口令加密快照，返回可公开内嵌的元数据与密文。"""
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    iterations = 100000
    salt = os.urandom(16)
    iv = os.urandom(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"),
                                  salt, iterations, dklen=64)
    enc_key, mac_key = derived[:32], derived[32:]

    proc = subprocess.run(
        ["openssl", "enc", "-aes-256-cbc",
         "-K", enc_key.hex(), "-iv", iv.hex()],
        input=raw, capture_output=True, timeout=30)
    if proc.returncode != 0:
        raise RuntimeError("OpenSSL 加密失败：%s" %
                           proc.stderr.decode("utf-8", "replace").strip())
    cipher = proc.stdout
    tag = hmac.new(mac_key, iv + cipher, hashlib.sha256).digest()
    return {
        "encrypted": True,
        "algorithm": "PBKDF2-HMAC-SHA256 + AES-256-CBC + HMAC-SHA256",
        "iterations": iterations,
        "salt": b64e(salt),
        "iv": b64e(iv),
        "data": b64e(cipher),
        "tag": b64e(tag),
    }


def build_static_snapshot(db_path, out_dir, password=None):
    """
    生成自包含静态网页到 out_dir/index.html。
    设置 password 时，帖子数据会加密后内嵌，适合公网静态托管。
    返回写入文件的绝对路径。
    """
    posts = query_all_posts(db_path)
    github_token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    payload = {
        "posts": posts,
        "total": len(posts),
        "synced_at": ts_to_str(int(time.time() * 1000)),
        "repo": "licht0/xueqiu-monitor",
        "workflow": "xueqiu.yml",
        "github_token": github_token,
    }
    embedded_payload = encrypt_snapshot_payload(payload, password) if password else payload
    data_json = json.dumps(embedded_payload, ensure_ascii=False)
    data_json = (data_json.replace("&", "\\u0026")
                          .replace("<", "\\u003c")
                          .replace(">", "\\u003e")
                          .replace("\u2028", "\\u2028")
                          .replace("\u2029", "\\u2029"))

    empty_data_tag = '<script id="snapshot-data" type="application/json"></script>'
    data_tag = ('<script id="snapshot-data" type="application/json">%s</script>'
                % data_json)
    page_html = WEB_PAGE_HTML
    assert empty_data_tag in page_html, "未找到快照 JSON 标签"
    page_html = page_html.replace(empty_data_tag, data_tag, 1)

    if password:
        assert "<body>" in page_html
        page_html = page_html.replace("<body>", "<body>\n" + _GATE_HTML, 1)

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "index.html")
    tmp_path = out_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(page_html)
    os.replace(tmp_path, out_path)
    return out_path


def deploy_snapshot(snapshot_dir):
    """
    若快照目录是带远程的 git 仓库，则自动提交并推送（适配 GitHub Pages 等）。
    返回 (是否执行, 说明)。未配置时静默跳过。
    """
    def git(*args):
        return subprocess.run(
            ["git", *args], cwd=snapshot_dir, capture_output=True,
            text=True, timeout=120)

    if not os.path.isdir(os.path.join(snapshot_dir, ".git")):
        return False, "快照目录未初始化 git，跳过自动部署"

    r = git("remote")
    if not r.stdout.strip():
        return False, "快照目录没有配置远程仓库，跳过自动部署"

    git("add", "-A")
    r = git("diff", "--cached", "--quiet")
    if r.returncode == 0:
        return True, "无内容变化，无需推送"

    git("commit", "-m", "snapshot %s" % time.strftime("%Y-%m-%d %H:%M:%S"))
    r = git("push")
    if r.returncode != 0:
        return False, "推送失败：%s" % (r.stderr.strip() or r.stdout.strip())
    return True, "已提交并推送到远程"


WEB_PAGE_HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>段永平雪球发言监控</title>
<style>
:root{
  --primary:#0f8a6c; --ink:#1f2329; --sub:#646a73;
  --line:#e7e9ec; --bg:#f4f5f6; --link:#2b6cb0;
}
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{
  margin:0; background:var(--bg); color:var(--ink);
  font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Helvetica Neue","Microsoft YaHei",sans-serif;
  line-height:1.7;
}
.wrap{max-width:720px;margin:0 auto;padding:0 16px}
header.topbar{background:#fff;border-bottom:1px solid var(--line);position:sticky;top:0;z-index:20}
header.topbar .wrap{display:flex;justify-content:space-between;align-items:center;min-height:52px;gap:12px}
.brand{font-weight:600;font-size:15px;white-space:nowrap}
.brand .dot{color:var(--primary);margin-right:2px}
.brand small{color:var(--sub);font-weight:400;font-size:12px;margin-left:6px}
.stats-line{color:var(--sub);font-size:12.5px;text-align:right}
.banner{
  display:none; background:var(--primary); color:#fff; text-align:center;
  padding:9px 12px; font-size:14px; cursor:pointer;
}
.banner:hover{background:#0d7a5f}
.banner.show{display:block}
main{padding-bottom:40px}
.card{
  background:#fff; border:1px solid var(--line); border-radius:10px;
  padding:14px 16px; margin:10px 0;
}
.card-time{color:var(--sub);font-size:12.5px;margin-bottom:8px}
.card-body{font-size:14.5px;word-break:break-word}
.card-body a{color:var(--link);text-decoration:none}
.card-body a:hover{text-decoration:underline}
.card-body img.emoji{height:1.3em;width:auto;vertical-align:middle;margin:0 1px}
.card-body img:not(.emoji){max-width:100%;max-height:480px;border-radius:6px;cursor:zoom-in;display:block;margin:8px 0}
.post-images{margin-top:10px;display:flex;flex-wrap:wrap;gap:8px}
.post-images img{
  max-width:100%; max-height:520px; width:auto; height:auto;
  border-radius:6px; display:block; cursor:zoom-in;
}
.post-images img.single{max-width:100%;max-height:640px}
.quote{
  margin-top:10px; background:#f7f8f9; border:1px solid var(--line);
  border-left:3px solid var(--primary); border-radius:6px; padding:10px 12px;
}
.quote-head{color:var(--sub);font-size:12.5px;margin-bottom:6px}
.quote-head b{color:var(--ink);font-weight:600}
.quote-body{font-size:13.5px;color:#3c434d;word-break:break-word}
.quote-body a{color:var(--link);text-decoration:none}
.quote-body img.emoji{height:1.2em;width:auto;vertical-align:middle}
.quote-body img:not(.emoji){max-width:100%;max-height:360px;border-radius:5px;cursor:zoom-in;display:block;margin:6px 0}
.quote-body img.qimg{
  max-width:100%;max-height:400px;border-radius:5px;margin-top:6px;display:block;cursor:zoom-in;
}
.conv{
  margin-top:10px; border-left:2px solid var(--line);
  padding-left:10px; color:#5a6069; font-size:13px;
}
.conv .conv-item{margin:4px 0}
.conv .conv-user{color:var(--primary);font-weight:600}
.card-link{display:inline-block;margin-top:10px;color:var(--sub);font-size:12.5px;text-decoration:none}
.card-link:hover{color:var(--link);text-decoration:underline}
.empty{text-align:center;color:var(--sub);padding:80px 0;font-size:14px}
@media (max-width:520px){
  .card{padding:12px 14px}
  .brand small{display:none}
}
/* 图片浮层预览 */
.lightbox{
  display:none; position:fixed; inset:0; z-index:9999;
  background:rgba(0,0,0,0.88); align-items:center; justify-content:center;
  cursor:zoom-out; padding:20px;
}
.lightbox.show{display:flex}
.lightbox img{
  max-width:100%; max-height:100%; object-fit:contain;
  border-radius:4px; box-shadow:0 4px 30px rgba(0,0,0,0.5);
}
.lightbox .lb-close{
  position:absolute; top:14px; right:20px; color:#fff; font-size:32px;
  line-height:1; cursor:pointer; background:none; border:none; opacity:0.8;
}
.lightbox .lb-close:hover{opacity:1}
.lightbox .lb-counter{
  position:absolute; bottom:16px; left:50%; transform:translateX(-50%);
  color:#fff; font-size:14px; opacity:0.7;
}
.lightbox .lb-nav{
  position:absolute; top:50%; transform:translateY(-50%);
  color:#fff; font-size:40px; cursor:pointer; background:none; border:none;
  opacity:0.6; padding:0 16px; user-select:none;
}
.lightbox .lb-nav:hover{opacity:1}
.lightbox .lb-prev{left:10px}
.lightbox .lb-next{right:10px}
.topbar-actions{display:flex;align-items:center;gap:10px}
.refresh-btn{
  background:var(--primary); color:#fff; border:none; border-radius:6px;
  padding:6px 14px; font-size:13px; cursor:pointer; white-space:nowrap;
  transition:opacity 0.2s;
}
.refresh-btn:hover{opacity:0.85}
.refresh-btn:disabled{opacity:0.5;cursor:not-allowed}
</style>
</head>
<body>
<header class="topbar">
  <div class="wrap">
    <div class="brand"><span class="dot">●</span>雪球发言监控<small>大道无形我有型 · 段永平</small></div>
    <div class="topbar-actions">
      <div class="stats-line" id="statsLine">正在加载…</div>
      <button class="refresh-btn" id="refreshBtn" hidden>抓取最新</button>
    </div>
  </div>
</header>
<div class="banner" id="newBanner"></div>
<main class="wrap">
  <div id="feed"></div>
  <div class="empty" id="emptyTip" hidden>暂无记录，程序运行后抓到的发言会显示在这里。</div>
</main>
<div class="lightbox" id="lightbox">
  <button class="lb-close" id="lbClose">&times;</button>
  <button class="lb-nav lb-prev" id="lbPrev">&#8249;</button>
  <img id="lbImg" src="" alt="">
  <button class="lb-nav lb-next" id="lbNext">&#8250;</button>
  <div class="lb-counter" id="lbCounter"></div>
</div>
<script id="snapshot-data" type="application/json"></script>
<script>
(function(){
  var SNAPSHOT = (function(){
    try{
      return JSON.parse(document.getElementById('snapshot-data').textContent);
    }catch(e){ return null; }
  })();
  var feed = document.getElementById('feed');
  var emptyTip = document.getElementById('emptyTip');
  var statsLine = document.getElementById('statsLine');
  var banner = document.getElementById('newBanner');
  var newestId = null, total = 0;

  function el(tag, cls, text){
    var e = document.createElement(tag);
    if(cls) e.className = cls;
    if(text != null) e.textContent = text;
    return e;
  }
  function extLink(href, text, cls){
    var a = el('a', cls, text);
    a.href = href; a.target = '_blank'; a.rel = 'noopener noreferrer';
    return a;
  }

  // 雪球 HTML 净化：移除脚本，补全协议相对 URL，标记表情图
  function sanitizeHtml(raw){
    if(!raw) return '';
    var doc = new DOMParser().parseFromString(raw, 'text/html');
    doc.querySelectorAll('script, style, iframe').forEach(function(n){ n.remove(); });
    doc.querySelectorAll('img').forEach(function(img){
      var src = img.getAttribute('src') || '';
      if(src.indexOf('//') === 0) src = 'https:' + src;
      else if(src.indexOf('/') === 0) src = 'https://xueqiu.com' + src;
      src = src.replace(/!thumb\.[a-z]+$/i, '');
      img.setAttribute('src', src);
      img.setAttribute('referrerpolicy', 'no-referrer');
      var s = src.toLowerCase();
      if(s.indexOf('face/emoji') !== -1 || s.indexOf('assets.imedao.com/ugc/images') !== -1){
        img.className = 'emoji';
      }
      img.removeAttribute('onerror'); img.removeAttribute('onload');
    });
    doc.querySelectorAll('a').forEach(function(a){
      var href = a.getAttribute('href') || '';
      if(href.indexOf('//') === 0) href = 'https:' + href;
      a.setAttribute('href', href);
      a.setAttribute('target', '_blank');
      a.setAttribute('rel', 'noopener noreferrer');
    });
    return doc.body.innerHTML;
  }

  // 将 text 中的 "//@用户: 内容" 对话链拆分为独立上下文块
  function splitConversation(text){
    if(!text) return {main: '', conv: []};
    // 匹配 "回复 @用户: " 开头（直接回复）
    var replyMatch = text.match(/^回复\s*@([^:：]+)[:：]\s*/);
    var main = text;
    if(replyMatch){
      main = text.slice(replyMatch[0].length);
    }
    var parts = main.split(/\/\/@/);
    var body = parts.shift() || '';
    var conv = parts.map(function(p){
      var m = p.match(/^([^:：]+)[:：]\s*/);
      if(m) return {user: m[1], text: p.slice(m[0].length)};
      return {user: '', text: p};
    }).filter(function(c){ return c.text.trim(); });
    return {main: body.trim(), conv: conv};
  }

  // ---- 图片浮层预览 ----
  var lbImages = [];
  var lbIndex = 0;
  function openLightbox(urls, idx){
    lbImages = urls || [];
    lbIndex = idx || 0;
    if(!lbImages.length) return;
    document.getElementById('lbImg').src = lbImages[lbIndex];
    document.getElementById('lbCounter').textContent =
      lbImages.length > 1 ? (lbIndex + 1) + ' / ' + lbImages.length : '';
    document.getElementById('lightbox').classList.add('show');
    document.body.style.overflow = 'hidden';
  }
  function closeLightbox(){
    document.getElementById('lightbox').classList.remove('show');
    document.body.style.overflow = '';
  }
  function lbNav(dir){
    if(!lbImages.length) return;
    lbIndex = (lbIndex + dir + lbImages.length) % lbImages.length;
    document.getElementById('lbImg').src = lbImages[lbIndex];
    document.getElementById('lbCounter').textContent =
      lbImages.length > 1 ? (lbIndex + 1) + ' / ' + lbImages.length : '';
  }
  document.addEventListener('DOMContentLoaded', function(){
    var lb = document.getElementById('lightbox');
    document.getElementById('lbClose').addEventListener('click', closeLightbox);
    lb.addEventListener('click', function(e){
      if(e.target === lb || e.target.tagName === 'IMG') closeLightbox();
    });
    document.getElementById('lbPrev').addEventListener('click', function(e){
      e.stopPropagation(); lbNav(-1);
    });
    document.getElementById('lbNext').addEventListener('click', function(e){
      e.stopPropagation(); lbNav(1);
    });
    document.addEventListener('keydown', function(e){
      if(!lb.classList.contains('show')) return;
      if(e.key === 'Escape') closeLightbox();
      else if(e.key === 'ArrowLeft') lbNav(-1);
      else if(e.key === 'ArrowRight') lbNav(1);
    });
  });

  function renderImages(picStr, cls){
    if(!picStr) return null;
    var urls = picStr.split(',').map(function(u){
      u = u.trim();
      // 雪球缩略图 URL 带 !thumb.jpg 后缀，去掉获取原图
      return u.replace(/!thumb\.[a-z]+$/i, '');
    }).filter(function(u){ return u; });
    if(!urls.length) return null;
    var wrap = el('div', cls);
    urls.forEach(function(u, i){
      var img = document.createElement('img');
      img.src = u;
      img.setAttribute('referrerpolicy', 'no-referrer');
      img.setAttribute('loading', 'lazy');
      if(urls.length === 1) img.className = 'single';
      img.addEventListener('click', function(){ openLightbox(urls, i); });
      wrap.appendChild(img);
    });
    return wrap;
  }

  // 为 raw_html 中的内嵌图片绑定浮层预览（排除表情图）
  function bindInlineImages(container){
    var imgs = container.querySelectorAll('img:not(.emoji)');
    if(!imgs.length) return;
    var urls = Array.from(imgs).map(function(i){ return i.src; });
    imgs.forEach(function(img, i){
      img.style.cursor = 'zoom-in';
      img.addEventListener('click', function(e){
        e.preventDefault();
        openLightbox(urls, i);
      });
    });
  }

  function renderCard(p){
    var c = el('article', 'card');
    c.appendChild(el('div', 'card-time', p.time_str || ''));

    // 正文：优先用 raw_html 渲染（保留 @链接、表情图、格式）
    var body = el('div', 'card-body');
    if(p.raw_html){
      body.innerHTML = sanitizeHtml(p.raw_html);
      bindInlineImages(body);
    } else {
      body.textContent = p.text || '(无文字内容)';
    }
    c.appendChild(body);

    // 正文配图
    var mainImgs = renderImages(p.pic, 'post-images');
    if(mainImgs) c.appendChild(mainImgs);

    // 引用内容（retweeted_status）
    if(p.has_retweet){
      var q = el('div', 'quote');
      var h = el('div', 'quote-head');
      h.appendChild(document.createTextNode('@'));
      h.appendChild(el('b', null, p.rt_user || '未知用户'));
      if(p.rt_time_str) h.appendChild(document.createTextNode(' · ' + p.rt_time_str));
      q.appendChild(h);
      var qbody = el('div', 'quote-body');
      if(p.rt_raw_html){
        qbody.innerHTML = sanitizeHtml(p.rt_raw_html);
        bindInlineImages(qbody);
      } else {
        qbody.textContent = p.rt_text || '(原帖无文字)';
      }
      var qimgs = renderImages(p.rt_pic, 'post-images');
      if(qimgs) qbody.appendChild(qimgs);
      q.appendChild(qbody);
      if(p.rt_id){
        q.appendChild(extLink('https://xueqiu.com/' + p.rt_user_id + '/' + p.rt_id,
                              '查看原帖 →', 'card-link'));
      }
      c.appendChild(q);
    }

    // 前后文对话链（text 中的 //@用户: ...）
    var split = splitConversation(p.text || '');
    if(split.conv.length){
      var conv = el('div', 'conv');
      split.conv.forEach(function(item){
        var line = el('div', 'conv-item');
        if(item.user){
          line.appendChild(el('span', 'conv-user', '@' + item.user + ' '));
        }
        line.appendChild(document.createTextNode(item.text));
        conv.appendChild(line);
      });
      c.appendChild(conv);
    }

    c.appendChild(extLink(p.url, '雪球原帖 →', 'card-link'));
    return c;
  }

  // ---- 虚拟滚动：只渲染可视区域帖子，避免 11k+ DOM 卡顿 ----
  var allPosts = [];
  var CARD_EST = 180; // 预估卡片高度（px）
  var BUFFER = 6;     // 可视区外预渲染数量
  var cardHeights = []; // 记录已渲染卡片的真实高度
  var scrollTicking = false;

  function getCardHeight(i){
    return cardHeights[i] || CARD_EST;
  }
  function indexAtScrollTop(scrollTop){
    var acc = 0;
    for(var i = 0; i < allPosts.length; i++){
      acc += getCardHeight(i);
      if(acc > scrollTop) return i;
    }
    return allPosts.length;
  }
  function renderVisible(){
    var scrollTop = window.scrollY || document.documentElement.scrollTop;
    var viewH = window.innerHeight;
    var start = Math.max(0, indexAtScrollTop(scrollTop) - BUFFER);
    var end = Math.min(allPosts.length, indexAtScrollTop(scrollTop + viewH) + BUFFER);
    if(start === renderVisible._lastStart && end === renderVisible._lastEnd) return;
    renderVisible._lastStart = start;
    renderVisible._lastEnd = end;

    // 顶部占位：start 之前所有卡片高度之和
    var topPad = 0;
    for(var i = 0; i < start; i++) topPad += getCardHeight(i);
    // 底部占位：end 之后所有卡片高度之和
    var bottomPad = 0;
    for(var k = end; k < allPosts.length; k++) bottomPad += getCardHeight(k);

    feed.textContent = '';
    var topSpacer = document.createElement('div');
    topSpacer.style.height = topPad + 'px';
    feed.appendChild(topSpacer);

    for(var j = start; j < end; j++){
      var node = renderCard(allPosts[j]);
      feed.appendChild(node);
      // 测量真实高度，更新缓存
      (function(idx, el){
        requestAnimationFrame(function(){
          var h = el.offsetHeight;
          if(h && cardHeights[idx] !== h){
            cardHeights[idx] = h;
            renderVisible._needsUpdate = true;
          }
        });
      })(j, node);
    }

    var bottomSpacer = document.createElement('div');
    bottomSpacer.style.height = bottomPad + 'px';
    feed.appendChild(bottomSpacer);
  }
  function onScroll(){
    if(scrollTicking) return;
    scrollTicking = true;
    requestAnimationFrame(function(){
      renderVisible();
      scrollTicking = false;
    });
  }
  // 高度测量后按需重排
  setInterval(function(){
    if(renderVisible._needsUpdate){
      renderVisible._needsUpdate = false;
      renderVisible._lastStart = -1;
      renderVisible();
    }
  }, 400);

  function renderList(posts, append){
    if(!append){
      allPosts = posts.slice();
      cardHeights = new Array(posts.length);
      renderVisible._lastStart = -1;
      renderVisible();
      window.addEventListener('scroll', onScroll, {passive:true});
      window.addEventListener('resize', onScroll);
    }else{
      allPosts = allPosts.concat(posts);
      cardHeights = cardHeights.concat(new Array(posts.length));
      renderVisible._lastStart = -1;
      renderVisible();
    }
  }

  function updateStats(){
    var statusTime = SNAPSHOT
      ? '快照生成于 ' + SNAPSHOT.synced_at
      : '页面检查于 ' + new Date().toLocaleTimeString('zh-CN', {hour12:false});
    statsLine.textContent = '共 ' + total + ' 条 · ' + statusTime;
  }

  function getJSON(url){
    return fetch(url).then(function(r){
      if(!r.ok) throw new Error('HTTP ' + r.status);
      return r.json();
    });
  }

  function applyInitialPage(posts){
    if(posts.length){
      newestId = posts[0].id;
      renderList(posts, false);
    }else{
      emptyTip.hidden = false;
    }
    updateStats();
  }

  function loadInitial(){
    if(SNAPSHOT){
      total = SNAPSHOT.total;
      applyInitialPage(SNAPSHOT.posts || []);
      return Promise.resolve();
    }
    return getJSON('/api/posts?limit=10000').then(function(d){
      total = d.total;
      applyInitialPage(d.posts || []);
    });
  }

  function checkNew(){
    getJSON('/api/posts?limit=10000').then(function(d){
      total = d.total;
      updateStats();
      var fresh = (d.posts || []).filter(function(p){ return p.id > newestId; });
      if(fresh.length && newestId !== null){
        banner.textContent = '有 ' + fresh.length + ' 条新发言，点击查看';
        banner.onclick = function(){
          fresh.sort(function(a,b){ return a.created_at - b.created_at; });
          allPosts = fresh.concat(allPosts);
          cardHeights = new Array(fresh.length).concat(cardHeights);
          newestId = fresh[fresh.length - 1].id;
          renderVisible._lastStart = -1;
          renderVisible();
          banner.classList.remove('show');
          window.scrollTo({top:0, behavior:'smooth'});
        };
        banner.classList.add('show');
      }
    }).catch(function(){});
  }

  function b64ToBytes(value){
    return Uint8Array.from(atob(value), function(ch){ return ch.charCodeAt(0); });
  }
  function bytesToB64(buf){
    var bytes = new Uint8Array(buf);
    return btoa(String.fromCharCode.apply(null, Array.from(bytes)));
  }
  function joinBytes(a, b){
    var out = new Uint8Array(a.length + b.length);
    out.set(a, 0);
    out.set(b, a.length);
    return out;
  }

  async function decryptSnapshot(password){
    var iv = b64ToBytes(SNAPSHOT.iv);
    var cipher = b64ToBytes(SNAPSHOT.data);
    var salt = b64ToBytes(SNAPSHOT.salt);
    var passwordKey = await crypto.subtle.importKey(
      'raw', new TextEncoder().encode(password),
      {name:'PBKDF2'}, false, ['deriveBits']);
    var derivedBuf = await crypto.subtle.deriveBits({
      name:'PBKDF2', salt:salt,
      iterations:SNAPSHOT.iterations, hash:'SHA-256'
    }, passwordKey, 512);
    var derived = new Uint8Array(derivedBuf);

    var macKey = await crypto.subtle.importKey(
      'raw', derived.slice(32),
      {name:'HMAC', hash:'SHA-256'}, false, ['sign']);
    var actualTag = await crypto.subtle.sign('HMAC', macKey, joinBytes(iv, cipher));
    if(bytesToB64(actualTag) !== SNAPSHOT.tag) throw new Error('bad tag');

    var aesKey = await crypto.subtle.importKey(
      'raw', derived.slice(0, 32),
      {name:'AES-CBC'}, false, ['decrypt']);
    var plainBuf = await crypto.subtle.decrypt(
      {name:'AES-CBC', iv:iv}, aesKey, cipher);
    var decrypted = JSON.parse(new TextDecoder().decode(plainBuf));

    SNAPSHOT = decrypted;
    total = decrypted.total;
    applyInitialPage(decrypted.posts || []);
    document.getElementById('gate').style.display = 'none';
    setupRefresh();
  }
  window.__xqDecrypt = decryptSnapshot;

  // ---- 手动触发抓取：直接调用 GitHub API，完成后刷新页面 ----
  var refreshBtn = document.getElementById('refreshBtn');
  function triggerFetch(){
    if(!SNAPSHOT || !SNAPSHOT.github_token){
      window.open('https://github.com/' + SNAPSHOT.repo + '/actions/workflows/' + SNAPSHOT.workflow, '_blank');
      return;
    }
    refreshBtn.disabled = true;
    refreshBtn.textContent = '抓取中…';
    fetch('https://api.github.com/repos/' + SNAPSHOT.repo +
          '/actions/workflows/' + SNAPSHOT.workflow + '/dispatches', {
      method: 'POST',
      headers: {
        'Accept': 'application/vnd.github+json',
        'Authorization': 'Bearer ' + SNAPSHOT.github_token,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ ref: 'main' })
    }).then(function(r){
      if(r.status === 204){
        refreshBtn.textContent = '已触发，等待完成…';
        // 轮询最新一次运行结果，成功后刷新页面
        pollLatestRun();
      }else{
        refreshBtn.textContent = '触发失败(' + r.status + ')';
        refreshBtn.disabled = false;
        setTimeout(function(){ refreshBtn.textContent = '抓取最新'; }, 2000);
      }
    }).catch(function(){
      refreshBtn.textContent = '网络错误';
      refreshBtn.disabled = false;
      setTimeout(function(){ refreshBtn.textContent = '抓取最新'; }, 2000);
    });
  }
  function pollLatestRun(){
    var attempts = 0;
    var maxAttempts = 60; // 最多等 2 分钟
    var poll = setInterval(function(){
      attempts++;
      fetch('https://api.github.com/repos/' + SNAPSHOT.repo +
            '/actions/runs?per_page=1', {
        headers: {
          'Accept': 'application/vnd.github+json',
          'Authorization': 'Bearer ' + SNAPSHOT.github_token
        }
      }).then(function(r){ return r.json(); }).then(function(d){
        var run = d.workflow_runs && d.workflow_runs[0];
        if(run && run.status === 'completed'){
          clearInterval(poll);
          if(run.conclusion === 'success'){
            refreshBtn.textContent = '完成，刷新页面…';
            setTimeout(function(){ location.reload(); }, 800);
          }else{
            refreshBtn.textContent = '运行失败';
            refreshBtn.disabled = false;
            setTimeout(function(){ refreshBtn.textContent = '抓取最新'; }, 2000);
          }
        }else if(attempts >= maxAttempts){
          clearInterval(poll);
          refreshBtn.textContent = '超时，稍后刷新';
          refreshBtn.disabled = false;
          setTimeout(function(){ refreshBtn.textContent = '抓取最新'; }, 2000);
        }
      }).catch(function(){});
    }, 2000);
  }
  function setupRefresh(){
    refreshBtn.hidden = false;
    refreshBtn.addEventListener('click', triggerFetch);
  }

  if(SNAPSHOT && SNAPSHOT.encrypted){
    statsLine.textContent = '内容已加密，请输入口令';
  }else{
    loadInitial().catch(function(){
      statsLine.textContent = '数据加载失败，请确认程序正在运行';
    });
    setupRefresh();
    if(!SNAPSHOT) setInterval(checkNew, 60000);
  }
})();
</script>
</body>
</html>
"""


def _make_request_handler(db_path):
    class PanelHandler(BaseHTTPRequestHandler):
        # 静默：不往终端打印每个请求的访问日志
        def log_message(self, fmt, *args):
            return

        def _send(self, code, content_type, body):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path

            if path in ("/", "/index.html"):
                self._send(200, "text/html; charset=utf-8",
                           WEB_PAGE_HTML.encode("utf-8"))
                return

            if path == "/api/posts":
                qs = parse_qs(parsed.query)
                try:
                    limit = int(qs.get("limit", ["20"])[0])
                    before_raw = qs.get("before_id", [None])[0]
                    before_id = int(before_raw) if before_raw else None
                except (TypeError, ValueError):
                    self._send(400, "application/json; charset=utf-8",
                               b'{"error":"bad query parameters"}')
                    return
                limit = min(max(limit, 1), 10000)

                try:
                    posts, total = query_posts(db_path, limit, before_id)
                except sqlite3.Error as e:
                    body = json.dumps({"error": str(e)}, ensure_ascii=False)
                    self._send(500, "application/json; charset=utf-8",
                               body.encode("utf-8"))
                    return

                body = json.dumps({"posts": posts, "total": total},
                                  ensure_ascii=False).encode("utf-8")
                self._send(200, "application/json; charset=utf-8", body)
                return

            self._send(404, "text/plain; charset=utf-8", b"Not Found")

    return PanelHandler


class WebPanel:
    """守护线程方式运行的本地网页面板"""

    def __init__(self, db_path, port=8765):
        self.db_path = db_path
        self.requested_port = port
        self.port = None
        self.httpd = None
        self.thread = None

    def start(self):
        handler = _make_request_handler(self.db_path)
        # 端口 0 = 让系统分配可用端口（指定端口被占用时作为兜底）
        last_err = None
        for port in (self.requested_port, 0):
            try:
                self.httpd = ThreadingHTTPServer(("127.0.0.1", port), handler)
                break
            except OSError as e:
                last_err = e
                self.httpd = None
        if self.httpd is None:
            raise OSError("无法启动网页面板（端口 %s 被占用）: %s"
                          % (self.requested_port, last_err))

        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                      daemon=True, name="xq-webpanel")
        self.thread.start()
        return self.url

    @property
    def url(self):
        return "http://127.0.0.1:%d/" % (self.port or self.requested_port)

    def stop(self):
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
            self.httpd = None


# --------------------------------------------------------------------------- #
# 雪球客户端
# --------------------------------------------------------------------------- #

class WAFChallengeError(Exception):
    """遇到阿里云 WAF JS 挑战页"""


class AuthError(Exception):
    """cookie 失效 / 接口拒绝（400016）"""


class LoginRequiredError(Exception):
    """匿名会话无权访问该内容（雪球错误码 10022：请登录查看更多内容）"""


class XueqiuClient:
    def __init__(self, user_id, data_dir):
        self.user_id = user_id
        self.data_dir = data_dir
        self.cookie_path = os.path.join(data_dir, "cookies.json")
        self.cookiejar = CookieJar()
        ctx = ssl.create_default_context()
        self.ssl_ctx = ctx
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookiejar),
            urllib.request.HTTPSHandler(context=ctx),
        )

    # ---------------- cookie 管理 ---------------- #

    def _inject_cookies(self, cookies):
        """把 [{name,value,domain,path,http_only,secure}] 注入 CookieJar"""
        now = time.time()
        for c in cookies:
            domain = c.get("domain") or ".xueqiu.com"
            cookie = Cookie(
                version=0,
                name=c["name"],
                value=c.get("value", ""),
                port=None,
                port_specified=False,
                domain=domain,
                domain_specified=True,
                domain_initial_dot=domain.startswith("."),
                path=c.get("path", "/"),
                path_specified=True,
                secure=bool(c.get("secure", False)),
                expires=None,
                discard=True,
                comment=None,
                comment_url=None,
                rest={"HttpOnly": c.get("http_only", False)},
                rfc2109=False,
            )
            # 同名覆盖
            try:
                self.cookiejar.clear(cookie.domain, cookie.path, cookie.name)
            except KeyError:
                pass
            self.cookiejar.set_cookie(cookie)

    def _export_cookies(self):
        out = []
        for c in self.cookiejar:
            # Python 3.9 的 Cookie 用 _rest 存储非标准属性，3.10+ 才暴露 rest
            http_only = False
            if hasattr(c, "rest"):
                http_only = bool(getattr(c, "rest", {}).get("HttpOnly"))
            elif hasattr(c, "get_nonstandard_attr"):
                http_only = bool(c.get_nonstandard_attr("HttpOnly"))
            out.append({
                "name": c.name,
                "value": c.value,
                "domain": c.domain,
                "path": c.path,
                "http_only": http_only,
                "secure": c.secure,
            })
        return out

    def save_cookies(self):
        # 先写临时文件再替换，避免进程中途异常留下损坏文件
        tmp_path = self.cookie_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self._export_cookies(), f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self.cookie_path)

    def load_cookies(self):
        if not os.path.exists(self.cookie_path):
            return False
        try:
            with open(self.cookie_path, "r", encoding="utf-8") as f:
                cookies = json.load(f)
        except (json.JSONDecodeError, OSError):
            return False
        if not isinstance(cookies, list):
            return False
        self._inject_cookies(cookies)
        return True

    def set_manual_cookie(self, cookie_header):
        """解析浏览器复制的 Cookie 请求头: k1=v1; k2=v2"""
        cookies = []
        for item in cookie_header.split(";"):
            item = item.strip()
            if not item or "=" not in item:
                continue
            name, value = item.split("=", 1)
            cookies.append({
                "name": name.strip(),
                "value": value.strip(),
                "domain": ".xueqiu.com",
                "path": "/",
                "http_only": False,
                "secure": False,
            })
        self._inject_cookies(cookies)
        self.save_cookies()

    # ---------------- Playwright 自动引导 ---------------- #

    def bootstrap_with_playwright(self):
        """
        用无头 Chromium 打开用户主页，自动通过 WAF JS 挑战，导出全部 cookie。
        需要： pip3 install --user playwright && python3 -m playwright install chromium
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            log("未安装 playwright，无法自动通过雪球 WAF。")
            log("请执行以下两条命令安装（一次性，约 1-2 分钟）：")
            log("  pip3 install --user playwright")
            log("  python3 -m playwright install chromium")
            log("或者用 --cookie \"...\" 手动指定浏览器中的雪球 Cookie。")
            return False

        target_url = "%s/u/%d" % (BASE_URL, self.user_id)
        log("启动无头浏览器通过 WAF 挑战 ...")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                context = browser.new_context(user_agent=USER_AGENT, locale="zh-CN")
                page = context.new_page()
                page.goto(target_url, wait_until="domcontentloaded", timeout=60000)

                # WAF 挑战 JS 会计算 cookie 后自动刷新，最多等待 40 秒
                ok = False
                for _ in range(40):
                    time.sleep(1)
                    cookies = context.cookies()
                    name_set = {c["name"] for c in cookies}
                    title = page.title()
                    if ("ssxmod_itna" in name_set or "xq_a_token" in name_set) \
                            and "出错" not in title and "错误" not in title:
                        ok = True
                        break

                if not ok:
                    log("浏览器引导超时：WAF 挑战未通过（可能触发了风控）。")
                    log("可稍后重试，或改用 --cookie 手动指定。")
                    return False

                # 再访问一次 API，确认 cookie 真正可用
                cookies = context.cookies()
                self._inject_cookies([
                    {
                        "name": c["name"],
                        "value": c["value"],
                        "domain": c.get("domain", ".xueqiu.com"),
                        "path": c.get("path", "/"),
                        "http_only": c.get("httpOnly", False),
                        "secure": c.get("secure", False),
                    }
                    for c in cookies
                ])
                self.save_cookies()
                log("WAF 引导成功，已保存 %d 个 cookie。" % len(cookies))
                return True
            finally:
                browser.close()

    def interactive_login(self, timeout_sec=300):
        """
        弹出有界面的 Chromium，请用户扫码/手机号登录雪球；登录成功后自动保存 cookie。
        返回 True/False。
        """
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            log("未安装 playwright，无法弹出登录窗口。")
            log("请执行： pip3 install --user playwright && "
                "python3 -m playwright install chromium")
            log("或用 --cookie \"...\" 手动传入已登录账号的 Cookie。")
            return False

        login_url = "https://xueqiu.com/user/login"
        log("=" * 64)
        log("即将弹出浏览器窗口，请在窗口内用【雪球 App 扫码】或手机号登录。")
        log("登录成功后程序会自动继续，无需操作终端（等待上限 %d 秒）。" % timeout_sec)
        log("=" * 64)

        with sync_playwright() as p:
            # headless=False 才会显示窗口让用户扫码
            browser = p.chromium.launch(headless=False)
            try:
                context = browser.new_context(
                    user_agent=USER_AGENT, locale="zh-CN")
                page = context.new_page()
                page.goto(login_url, wait_until="domcontentloaded", timeout=60000)

                def verify_real_login():
                    """访客也会被种入 xq_a_token，必须同时满足：
                    1) 存在登录后才有的 xq_id_token；
                    2) 用该会话真实请求时间线第 2 页不返回 10022。
                    """
                    cs = context.cookies()
                    names = {c["name"] for c in cs}
                    if "xq_id_token" not in names:
                        return False, cs
                    check_url = "%s?user_id=%d&page=2&count=%d" % (
                        TIMELINE_API, self.user_id, PAGE_SIZE)
                    try:
                        resp = context.request.get(check_url, headers={
                            "User-Agent": USER_AGENT,
                            "Referer": "%s/u/%d" % (BASE_URL, self.user_id),
                            "X-Requested-With": "XMLHttpRequest",
                        }, timeout=15000)
                    except Exception:
                        return False, cs
                    if not resp.ok:
                        return False, cs
                    try:
                        body = resp.json()
                    except Exception:
                        return False, cs
                    if isinstance(body, dict) and body.get("error_code"):
                        return False, cs
                    if not (body.get("statuses") or []):
                        return False, cs
                    return True, context.cookies()

                ok = False
                waited = 0
                while waited < timeout_sec:
                    time.sleep(2)
                    waited += 2
                    ok, cookies = verify_real_login()
                    if ok:
                        break

                if not ok:
                    log("登录等待超时，未检测到有效登录态。")
                    return False

                self._inject_cookies([
                    {
                        "name": c["name"],
                        "value": c["value"],
                        "domain": c.get("domain", ".xueqiu.com"),
                        "path": c.get("path", "/"),
                        "http_only": c.get("httpOnly", False),
                        "secure": c.get("secure", False),
                    }
                    for c in cookies
                ])
                self.save_cookies()
                log("登录成功，已保存登录 cookie（共 %d 个）。" % len(cookies))
                return True
            finally:
                browser.close()

    def ensure_logged_in(self):
        """保证会话为登录态：已有登录 cookie 则直接用，否则弹窗登录"""
        if self.is_logged_in():
            return True
        # cookie 文件里可能是匿名会话，尝试用它翻第 2 页验证
        if not self.cookiejar and not self.load_cookies():
            if not self.bootstrap_with_playwright():
                return False
        if self.is_logged_in():
            return True
        return self.interactive_login()

    # ---------------- HTTP 请求 ---------------- #

    def _http_get(self, url):
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Referer": "%s/u/%d" % (BASE_URL, self.user_id),
            "X-Requested-With": "XMLHttpRequest",
        }
        req = urllib.request.Request(url, headers=headers)
        resp = self.opener.open(req, timeout=20)
        raw = resp.read()
        charset = resp.headers.get_content_charset() or "utf-8"
        return raw.decode(charset, errors="replace")

    def ensure_session(self, manual_cookie=None):
        """保证有一组可用 cookie"""
        if manual_cookie:
            self.set_manual_cookie(manual_cookie)
        elif not self.load_cookies():
            if not self.bootstrap_with_playwright():
                return False
        return True

    def fetch_timeline(self, page=1):
        url = "%s?user_id=%d&page=%d&count=%d" % (
            TIMELINE_API, self.user_id, page, PAGE_SIZE)

        # 雪球业务错误以 HTTP 400 + JSON 返回，需要读取响应体才能识别错误码
        try:
            text = self._http_get(url)
        except urllib.error.HTTPError as e:
            charset = e.headers.get_content_charset() or "utf-8"
            text = e.read().decode(charset, errors="replace")
            data = None
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                pass
            if isinstance(data, dict) and data.get("error_code"):
                code = str(data.get("error_code"))
                if code == "10022":
                    raise LoginRequiredError(
                        data.get("error_description", "请登录雪球查看更多内容"))
                if code == "400016":
                    raise AuthError(data.get("error_description", "cookie 失效"))
            raise RuntimeError("HTTP %d" % e.code)

        stripped = text.lstrip()
        if stripped.startswith("<"):
            if "_waf_" in text or "aliyun_waf" in text:
                raise WAFChallengeError("命中 WAF JS 挑战")
            raise RuntimeError("接口返回了非 JSON 页面")

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise RuntimeError("接口返回无法解析: %s" % e)

        if isinstance(data, dict) and data.get("error_code"):
            code = str(data.get("error_code"))
            if code == "400016":
                raise AuthError(data.get("error_description", "cookie 失效"))
            if code == "10022":
                raise LoginRequiredError(
                    data.get("error_description", "请登录雪球查看更多内容"))
            raise RuntimeError("接口错误 %s: %s" %
                               (code, data.get("error_description", "")))
        return data

    def fetch_status_detail(self, status_id):
        """获取单帖完整内容（用于时间线中被截断的引用帖）。
        返回 status dict，失败返回 None。"""
        url = "%s/statuses/show.json?id=%s" % (BASE_URL, status_id)
        try:
            text = self._http_get(url)
            data = json.loads(text)
            if isinstance(data, dict) and not data.get("error_code"):
                return data
        except Exception as e:
            log("抓取单帖详情 %s 失败: %s" % (status_id, e))
        return None

    def is_logged_in(self):
        """根据 cookie 判断当前会话是否为真实登录态。
        访客会话的 xq_id_token 中 uid=-1，必须明确拒绝。"""
        for c in self.cookiejar:
            if c.name != "xq_id_token" or not c.value:
                continue
            payload = decode_jwt_payload(c.value)
            uid = (payload or {}).get("uid")
            if isinstance(uid, int) and not isinstance(uid, bool) and uid > 0:
                return True
        return False


# --------------------------------------------------------------------------- #
# 解析
# --------------------------------------------------------------------------- #

def parse_status(status, user_id):
    """把 API 的 status 对象解析为存储记录。
    注意：雪球时间线接口的 description 字段会截断长帖，而 text 字段保留全文，
    因此正文/引用均优先使用 text，仅在缺失时回退到 description。"""
    rt = status.get("retweeted_status") or None
    rt_rec = None
    if rt and rt.get("id"):
        rt_user = rt.get("user") or {}
        rt_html = rt.get("text") or rt.get("description") or ""
        rt_pic = _normalize_pic(rt.get("pic") or rt.get("firstImg") or "")
        rt_rec = {
            "rt_id": rt.get("id"),
            "rt_user_id": rt.get("user_id"),
            "rt_user": rt_user.get("screen_name", ""),
            "rt_created_at": rt.get("created_at"),
            "rt_text": html_to_text(rt_html),
            "rt_raw_html": rt_html,
            "rt_pic": rt_pic,
        }

    reply_count = status.get("reply_count")
    if reply_count is None:
        reply_count = status.get("comment_count", 0) or 0

    main_html = status.get("text") or status.get("description") or ""
    rec = {
        "id": status.get("id"),
        "user_id": status.get("user_id", user_id),
        "created_at": status.get("created_at"),
        "source": status.get("source", ""),
        "title": status.get("title", "") or "",
        "text": html_to_text(main_html),
        "raw_html": main_html,
        "pic": _normalize_pic(status.get("pic") or status.get("firstImg") or ""),
        "has_retweet": rt_rec is not None,
        "rt_id": None,
        "rt_user_id": None,
        "rt_user": "",
        "rt_created_at": None,
        "rt_text": "",
        "rt_raw_html": "",
        "rt_pic": "",
        "reply_count": int(reply_count or 0),
        "retweet_count": int(status.get("retweet_count", 0) or 0),
        "like_count": int(status.get("like_count", 0) or 0),
        "url": "https://xueqiu.com/%d/%s" % (user_id, status.get("id")),
    }
    if rt_rec:
        rec.update(rt_rec)
    return rec


def _normalize_pic(pic_str):
    """雪球 pic 字段可能是逗号分隔的多图 URL，统一为逗号分隔字符串"""
    if not pic_str:
        return ""
    urls = [u.strip() for u in str(pic_str).split(",") if u.strip()]
    # 去掉缩略图尺寸后缀之外的脏数据
    return ",".join(urls)


def _is_truncated(text):
    """判断文本是否被接口截断（末尾 ... 且长度超过阈值）"""
    if not text:
        return False
    t = text.rstrip()
    return t.endswith("...") and len(t) > 60


def _enrich_with_detail(client, rec):
    """若正文或引用内容被截断，拉取单帖详情补全全文。
    雪球时间线的 text 字段对超长帖仍会截断，需通过 show.json 获取全文。"""
    # 正文
    if _is_truncated(rec.get("text")):
        detail = client.fetch_status_detail(rec["id"])
        if detail:
            full = detail.get("text") or detail.get("description") or ""
            if full and not _is_truncated(full):
                rec["text"] = html_to_text(full)
                rec["raw_html"] = full
                pic = _normalize_pic(detail.get("pic") or detail.get("firstImg") or "")
                if pic and not rec.get("pic"):
                    rec["pic"] = pic
    # 引用
    if rec.get("has_retweet") and _is_truncated(rec.get("rt_text")):
        detail = client.fetch_status_detail(rec["rt_id"])
        if detail:
            full = detail.get("text") or detail.get("description") or ""
            if full and not _is_truncated(full) and "删除" not in full:
                rec["rt_text"] = html_to_text(full)
                rec["rt_raw_html"] = full
                pic = _normalize_pic(detail.get("pic") or detail.get("firstImg") or "")
                if pic and not rec.get("rt_pic"):
                    rec["rt_pic"] = pic


def format_post(rec):
    """控制台友好打印"""
    sep = "=" * 78
    sub = "-" * 78
    lines = [
        "",
        sep,
        "[新发言] %s  来自 %s" % (ts_to_str(rec["created_at"]), rec["source"] or "雪球"),
    ]
    if rec["title"]:
        lines.append("标题: %s" % rec["title"])
    lines.append(rec["text"] or "(无文字内容)")

    if rec["has_retweet"]:
        lines.append(sub)
        lines.append("引用 @%s  %s" % (rec["rt_user"], ts_to_str(rec["rt_created_at"])))
        for ln in (rec["rt_text"] or "(原帖无文字)").split("\n"):
            lines.append("| " + ln if ln else "|")
        lines.append("| 原帖: https://xueqiu.com/%s/%s" %
                     (rec["rt_user_id"], rec["rt_id"]))

    lines.append(sub)
    lines.append("转发 %d | 评论 %d | 赞 %d" %
                 (rec["retweet_count"], rec["reply_count"], rec["like_count"]))
    lines.append("链接: " + rec["url"])
    lines.append(sep)
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #

def crawl_full_history(client, storage, user_id, data_dir, max_pages=1000,
                       page_delay=0.9, resume=False):
    """
    从第 1 页开始顺序翻页，直到某页返回空（或命中 max_pages 安全上限）。
    特性：
    - 每页瞬时错误自动重试（5 次，指数退避）
    - 登录失效(10022) 时重新弹窗登录一次后继续
    - 页间随机等待，降低触发风控概率
    - 进度写入 history_progress.json，抓取情况实时反映到网页面板
    - resume=True 时从进度文件中的下一页继续，避免重复抓取
    返回 (新入库数, 最后一页页码)。
    """
    progress_path = os.path.join(data_dir, "history_progress.json")
    new_total = 0
    login_refreshed = False
    page = 1
    if resume and os.path.exists(progress_path):
        try:
            with open(progress_path, "r", encoding="utf-8") as f:
                last_done = int(json.load(f).get("last_completed_page", 0))
            if last_done > 0:
                page = last_done + 1
                log("断点续抓：从第 %d 页开始（上次已完成第 %d 页）。"
                    % (page, last_done))
        except (ValueError, OSError, json.JSONDecodeError):
            page = 1

    while page <= max_pages:
        data = None
        last_err = None

        for attempt in range(5):
            try:
                data = client.fetch_timeline(page=page)
                break
            except LoginRequiredError as e:
                # 登录态丢了：重新登录（仅自动弹窗一次，避免死循环）
                if not login_refreshed:
                    log("抓取中登录态失效（%s），请重新扫码登录 ..." % e)
                    if client.interactive_login():
                        login_refreshed = True
                        continue
                raise
            except (WAFChallengeError, AuthError) as e:
                last_err = e
                wait = 3 * (attempt + 1)
                log("第 %d 页会话异常（%s），%d 秒后重试 ..." % (page, e, wait))
                time.sleep(wait)
            except (urllib.error.URLError, ConnectionError, OSError, RuntimeError) as e:
                last_err = e
                wait = 2 * (attempt + 1)
                log("第 %d 页请求异常（%s），%d 秒后重试 ..." % (page, e, wait))
                time.sleep(wait)

        if data is None:
            raise RuntimeError("第 %d 页连续 5 次抓取失败：%s" % (page, last_err))

        statuses = data.get("statuses") or []
        if not statuses:
            log("第 %d 页为空，历史已全部抓完。" % page)
            break

        page_new = 0
        for st in statuses:
            rec = parse_status(st, user_id)
            _enrich_with_detail(client, rec)
            if storage.upsert(rec):
                page_new += 1
                new_total += 1
                storage.append_jsonl(rec)

        oldest = statuses[-1]
        # 断点进度（始终从第 1 页重抓保证不遗漏；该文件用于观察进度/人工续跑参考）
        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump({
                "last_completed_page": page,
                "oldest_post_id": oldest.get("id"),
                "oldest_post_time": ts_to_str(oldest.get("created_at")),
                "db_total": storage.count(),
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }, f, ensure_ascii=False, indent=2)

        if page == 1 or page % 10 == 0:
            log("进度：已抓 %d 页 / 本页新增 %d 条 / 库内共 %d 条 / "
                "已回溯到 %s" %
                (page, page_new, storage.count(),
                 ts_to_str(oldest.get("created_at"))))

        page += 1
        # 随机抖动：page_delay ± 60%
        time.sleep(max(0.3, page_delay + random.uniform(-0.5, 0.5) * page_delay))

    else:
        log("达到安全页数上限 %d，停止（如确实需要更多可用 --max-pages 调整）。"
            % max_pages)

    return new_total, page - 1


def run(args):
    data_dir = os.path.abspath(args.data_dir)
    os.makedirs(data_dir, exist_ok=True)
    db_path = os.path.join(data_dir, "xueqiu.db")
    jsonl_path = os.path.join(data_dir, "posts.jsonl")

    storage = Storage(db_path, jsonl_path)
    client = XueqiuClient(args.user_id, data_dir)

    # 启动网页面板（默认开启；--no-web 关闭）
    panel = None
    if not args.no_web:
        panel = WebPanel(db_path, args.web_port)
        try:
            panel.start()
        except OSError as e:
            log("网页面板启动失败：%s" % e)
            panel = None

    consecutive_auth_fails = 0

    # 静态快照：目录与口令（口令也可用环境变量 SNAPSHOT_PASSWORD）
    snapshot_dir = os.path.abspath(args.snapshot_dir)
    snap_password = args.snapshot_password or os.environ.get("SNAPSHOT_PASSWORD")

    def refresh_snapshot(silent=False):
        try:
            path = build_static_snapshot(db_path, snapshot_dir, snap_password)
            did, msg = deploy_snapshot(snapshot_dir)
            if not silent:
                log("静态快照已更新：%s（%s）" % (path, msg))
            return True
        except Exception as e:
            if not silent:
                log("静态快照生成失败：%s" % e)
            return False

    def bootstrap():
        if not client.ensure_session(manual_cookie=args.cookie):
            return False
        try:
            client.fetch_timeline(page=1)
            return True
        except (WAFChallengeError, AuthError):
            log("现有 cookie 已失效，尝试重新引导 ...")
            # 清掉旧 cookie 重来
            if os.path.exists(client.cookie_path):
                os.remove(client.cookie_path)
            client.cookiejar.clear()
            return client.ensure_session(manual_cookie=args.cookie)

    if not bootstrap():
        log("无法建立雪球会话，程序退出。")
        return 1

    if panel:
        log("网页面板：%s （用浏览器打开，无需盯着终端）" % panel.url)

    if args.require_login:
        if not client.is_logged_in():
            log("未检测到真实雪球登录态，任务停止。")
            return 1
        try:
            client.fetch_timeline(page=2)
        except Exception as e:
            log("雪球登录态验证失败：%s" % e)
            return 1

    # ---------------- 全量历史一次性抓取 ---------------- #
    if args.full_history:
        log("模式：全量历史抓取（需登录雪球账号）。")

        # 登录尝试：首轮 + 自动重弹 2 轮
        logged_in = client.ensure_logged_in()
        login_round = 1
        while not logged_in and login_round < 3:
            login_round += 1
            log("第 %d 轮登录：3 秒后重新弹出浏览器窗口 ..." % login_round)
            time.sleep(3)
            logged_in = client.interactive_login()

        def keep_panel_alive():
            if panel:
                log("网页面板保持运行：%s" % panel.url)
                log("按 Ctrl+C 退出程序。")
                try:
                    while True:
                        time.sleep(3600)
                except KeyboardInterrupt:
                    log("收到中断信号，正在退出 ...")
                finally:
                    panel.stop()
                    storage.close()
                return 0
            storage.close()
            return 0 if panel is None else 1

        if not logged_in:
            log("未能完成登录。已有数据仍可在网页浏览；需要继续抓取时回复"
                "\"重新登录\"即可。")
            refresh_snapshot()
            return keep_panel_alive()

        try:
            new_n, last_page = crawl_full_history(
                client, storage, args.user_id, data_dir,
                max_pages=max(1, args.max_pages), resume=args.resume)
        except LoginRequiredError:
            log("仍无权限抓取历史。")
            refresh_snapshot()
            return keep_panel_alive()
        log("全量历史抓取完成：翻至第 %d 页，本次新入库 %d 条，库内共 %d 条。"
            % (last_page, new_n, storage.count()))
        refresh_snapshot()
        return keep_panel_alive()

    log("开始监控 uid=%d ，轮询间隔 %d 秒，数据目录 %s"
        % (args.user_id, args.interval, data_dir))

    # ---------------- 首次抓取：建基线 / 回填 ---------------- #
    def fetch_and_store(pages, announce):
        """抓 pages 页；announce=False 时静默入库(建基线)。返回新帖数。"""
        new_count = 0
        known_streak = 0
        for page in range(1, pages + 1):
            data = client.fetch_timeline(page=page)
            statuses = data.get("statuses") or []
            if not statuses:
                break
            for st in statuses:
                rec = parse_status(st, args.user_id)
                _enrich_with_detail(client, rec)
                is_new = storage.upsert(rec)
                if is_new:
                    new_count += 1
                    known_streak = 0
                    storage.append_jsonl(rec)
                    if announce:
                        print(format_post(rec))
                else:
                    known_streak += 1
            # 连续一整页都是已知帖子，更早的页不必再看
            if known_streak >= len(statuses) and page >= 1:
                break
        return new_count

    first_pages = max(1, args.backfill)
    n = fetch_and_store(first_pages, announce=args.backfill > 0)
    if args.backfill > 0:
        log("首次回填完成：新入库 %d 条，库内共 %d 条。" % (n, storage.count()))
    else:
        log("基线已建立：记录现有 %d 条发言，之后仅提示新发言。" % storage.count())
    refresh_snapshot(silent=(n == 0))

    if args.once:
        if panel:
            # 抓取结束，但网页面板保持运行，方便在浏览器里慢慢看
            log("抓取结束，网页面板保持运行：%s" % panel.url)
            log("按 Ctrl+C 退出程序。")
            try:
                while True:
                    time.sleep(3600)
            except KeyboardInterrupt:
                log("收到中断信号，正在退出 ...")
            finally:
                panel.stop()
                storage.close()
            return 0
        log("--once 模式，抓取结束。")
        storage.close()
        return 0

    # ---------------- 轮询循环 ---------------- #
    pages = max(1, args.pages)
    backoff = 0
    try:
        while True:
            time.sleep(max(args.interval, 10) if backoff == 0 else backoff)
            try:
                n = fetch_and_store(pages, announce=True)
                if n:
                    log("发现 %d 条新发言，库内共 %d 条。" % (n, storage.count()))
                else:
                    log("无新发言（最近检查时间 %s）。"
                        % time.strftime("%H:%M:%S"))
                refresh_snapshot()
                backoff = 0
                consecutive_auth_fails = 0
            except (WAFChallengeError, AuthError) as e:
                consecutive_auth_fails += 1
                log("会话失效（%s），第 %d 次尝试重新引导 ..."
                    % (e, consecutive_auth_fails))
                if os.path.exists(client.cookie_path):
                    os.remove(client.cookie_path)
                client.cookiejar.clear()
                if args.cookie:
                    client.set_manual_cookie(args.cookie)
                else:
                    if not client.bootstrap_with_playwright():
                        backoff = 60
                if consecutive_auth_fails >= 5:
                    log("连续引导失败，请检查网络或改用 --cookie 手动指定。程序退出。")
                    return 2
            except (urllib.error.URLError, RuntimeError) as e:
                backoff = min(backoff * 2 if backoff else 15, 120)
                log("请求异常：%s；%d 秒后重试。" % (e, backoff))
    except KeyboardInterrupt:
        log("收到中断信号，正在退出 ...")
    finally:
        if panel:
            panel.stop()
        storage.close()
    return 0


def parse_args():
    p = argparse.ArgumentParser(
        description="近实时监控雪球用户发言（默认：段永平 大道无形我有型）")
    p.add_argument("--user-id", type=int, default=DEFAULT_USER_ID,
                   help="雪球用户数字 ID，默认 %(default)s（段永平）")
    p.add_argument("--interval", type=int, default=3600,
                   help="轮询间隔秒数，默认 %(default)s（1 小时）")
    p.add_argument("--pages", type=int, default=1,
                   help="每次轮询向前检查的页数，默认 %(default)s")
    p.add_argument("--backfill", type=int, default=0,
                   help="启动时回填历史的页数（默认 0：仅建基线，不通知历史）")
    p.add_argument("--data-dir", default="data",
                   help="数据存放目录，默认 %(default)s")
    p.add_argument("--cookie", default=None,
 help='手动 Cookie，如 "xq_a_token=..; ssxmod_itna=.."')
    p.add_argument("--full-history", action="store_true",
                   help="一次性抓取该用户全部历史帖子（需扫码登录），完成后网页面板保持运行")
    p.add_argument("--resume", action="store_true",
                   help="配合 --full-history 使用：从 history_progress.json 断点续抓")
    p.add_argument("--max-pages", type=int, default=1000,
                   help="全量历史抓取的页数安全上限，默认 %(default)s")
    p.add_argument("--once", action="store_true",
                   help="只抓取一次然后退出（默认仍保持网页面板运行）")
    p.add_argument("--web-port", type=int, default=8765,
                   help="网页面板端口，默认 %(default)s；被占用时自动改用可用端口")
    p.add_argument("--no-web", action="store_true",
                   help="不启动网页面板，仅使用终端输出")
    p.add_argument("--require-login", action="store_true",
                   help="启动时要求真实雪球登录态，否则直接失败（供云端定时任务使用）")
    p.add_argument("--snapshot-dir", default="site",
                   help="静态快照输出目录，默认 %(default)s；该目录若配置了 git "
                        "远程，会自动提交推送")
    p.add_argument("--snapshot-password", default=None,
                   help="静态网页访问口令（也可用环境变量 SNAPSHOT_PASSWORD）")
    return p.parse_args()


if __name__ == "__main__":
    sys.exit(run(parse_args()))
