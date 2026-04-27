#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书知识库 PDF 批量导出工具

流程：
  1. 获取知识空间列表        → 得到 space_id
  2. 递归获取子节点列表      → 得到 obj_token + obj_type + title
  3. 调用导出 API            → 传入 obj_token + obj_type → 得到 ticket
  4. 轮询导出任务            → 传入 ticket + obj_token → 等待 job_status=0 → 得到 file_token
  5. 下载文件                → 传入 file_token → 保存 PDF（10 分钟内有效）

用法：
  python feishu_wiki_exporter.py
"""

import os
import re
import sys
import time
import json
import requests
from pathlib import Path
from typing import Optional


# ━━━━━━━━━━━━━━━━━━ 配置 ━━━━━━━━━━━━━━━━━━

APP_ID = "cli_a961321c35b8dbd2"
APP_SECRET = "KHaErPv5xq74iPdxKUVIAgEhB4JQBs4L"
EXPORT_DIR = "E:\desktop\本地知识库"

BASE_URL = "https://open.feishu.cn/open-apis"

# 导出 API 支持的 type 与 file_extension 对照
# obj_type → (export_type, file_extension)
EXPORT_TYPE_MAP = {
    "doc":   ("doc",   "pdf"),
    "docx":  ("docx",  "pdf"),
    "sheet": ("sheet", "xlsx"),  # sheet 不支持 pdf，导出 xlsx
    "bitable": ("bitable", "csv"),
}

# ━━━━━━━━━━━━━━━━━━ 工具函数 ━━━━━━━━━━━━━━━━━━


def log(msg: str, level: str = "INFO"):
    print(f"[{level}] {msg}", flush=True)


def clean_filename(name: str) -> str:
    """清理文件名中的非法字符"""
    return re.sub(r'[\\/:*?"<>|\r\n\t]', '', name).strip() or "untitled"


# ━━━━━━━━━━━━━━━━━━ 飞书 API ━━━━━━━━━━━━━━━━━━


class FeishuAPI:
    """飞书开放平台 API 封装"""

    def __init__(self, app_id: str, app_secret: str):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token: Optional[str] = None
        self.token_expires = 0.0
        self._last_request = 0.0

    # ---- 认证 ----

    def ensure_token(self):
        """确保 access_token 可用（自动刷新）"""
        if self.token and time.time() < self.token_expires - 60:
            return
        log("获取 tenant_access_token ...")
        resp = requests.post(
            f"{BASE_URL}/auth/v3/tenant_access_token/internal",
            json={"app_id": self.app_id, "app_secret": self.app_secret},
            timeout=10,
        )
        data = resp.json()
        if data.get("code") != 0:
            log(f"Token 获取失败: code={data.get('code')} msg={data.get('msg')}", "ERROR")
            sys.exit(1)
        self.token = data["tenant_access_token"]
        self.token_expires = time.time() + data.get("expire", 7200)
        log(f"Token 获取成功，有效期 {data.get('expire', 7200)}s")

    # ---- 通用请求 ----

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def call(self, method: str, path: str, **kwargs) -> Optional[dict]:
        """
        调用飞书 API，返回完整 JSON 响应（不吞错误）。
        code!=0 时也返回完整 body，由调用方判断。
        网络异常时返回 None。
        """
        self.ensure_token()

        # 简易限流：请求间隔 ≥ 100ms
        gap = 0.1 - (time.time() - self._last_request)
        if gap > 0:
            time.sleep(gap)

        kwargs.setdefault("headers", self._headers())
        kwargs.setdefault("timeout", 30)

        try:
            resp = requests.request(method, BASE_URL + path, **kwargs)
            self._last_request = time.time()
            return resp.json()
        except requests.exceptions.RequestException as e:
            log(f"网络异常: {e}", "ERROR")
            return None
        except ValueError:
            log(f"响应非 JSON: {resp.text[:200]}", "ERROR")
            return None

    # ---- 知识空间 ----

    def list_spaces(self) -> list:
        """获取所有知识空间"""
        spaces = []
        page_token = None
        while True:
            params = {"page_size": 50}
            if page_token:
                params["page_token"] = page_token
            body = self.call("GET", "/wiki/v2/spaces", params=params)
            if not body or body.get("code") != 0:
                log(f"获取空间列表失败: {body}", "ERROR")
                break
            spaces.extend(body["data"].get("items", []))
            if not body["data"].get("has_more"):
                break
            page_token = body["data"].get("page_token")
            if not page_token:
                break
        return spaces

    # ---- 节点遍历 ----

    def list_nodes(self, space_id: str, parent: str = None, path: list = None) -> list:
        """
        递归获取知识空间内所有可导出的文档节点。

        返回列表中每个元素：
          {
            "title":     文档标题,
            "obj_token": 文档实际 token（用于导出 API）,
            "obj_type":  文档类型（doc/docx/sheet/...）,
            "path":      文件夹路径（用于构建本地目录）,
          }
        """
        if path is None:
            path = []

        results = []
        page_token = None

        while True:
            params = {"page_size": 50}
            if parent:
                params["parent_node_token"] = parent
            if page_token:
                params["page_token"] = page_token

            body = self.call("GET", f"/wiki/v2/spaces/{space_id}/nodes", params=params)
            if not body or body.get("code") != 0:
                log(f"获取节点列表失败 (space={space_id}, parent={parent}): "
                    f"code={body.get('code') if body else 'N/A'} msg={body.get('msg') if body else 'N/A'}", "ERROR")
                break

            for item in body["data"].get("items", []):
                # 文件夹 → 递归
                if item.get("has_child"):
                    sub = self.list_nodes(
                        space_id,
                        item["node_token"],
                        path + [item.get("title", "")],
                    )
                    results.extend(sub)
                    continue

                # 文档节点 → 收集 obj_token 和 obj_type
                obj_type = item.get("obj_type", "")
                obj_token = item.get("obj_token", "")

                if obj_token and obj_type in EXPORT_TYPE_MAP:
                    results.append({
                        "title": item.get("title", "未命名"),
                        "obj_token": obj_token,
                        "obj_type": obj_type,
                        "path": path.copy(),
                    })
                elif obj_token and obj_type not in EXPORT_TYPE_MAP:
                    log(f"  ⏭ 不支持导出类型: {item.get('title')} (obj_type={obj_type})", "DEBUG")

            if not body["data"].get("has_more"):
                break
            page_token = body["data"].get("page_token")
            if not page_token:
                break

        return results

    # ---- 导出 ----

    def create_export_task(self, obj_token: str, obj_type: str) -> Optional[str]:
        """
        创建导出任务，返回 ticket；失败返回 None。

        关键：token 传 obj_token（不是 node_token），type 传 obj_type。
        """
        export_type, file_ext = EXPORT_TYPE_MAP[obj_type]
        body = self.call("POST", "/drive/v1/export_tasks", json={
            "token": obj_token,
            "type": export_type,
            "file_extension": file_ext,
        })

        if not body:
            return None
        if body.get("code") != 0:
            log(f"  创建导出任务失败: code={body.get('code')} msg={body.get('msg')}", "ERROR")
            return None

        ticket = body.get("data", {}).get("ticket")
        if ticket:
            log(f"  导出任务已创建: ticket={ticket}")
        return ticket

    def poll_export_task(self, ticket: str, obj_token: str) -> Optional[str]:
        """
        轮询导出任务，返回 file_token；失败/超时返回 None。

        ⚠️ 飞书 API 响应结构：
          {
            "code": 0,
            "data": {
              "result": {
                "job_status": 0,        ← 注意：是 data.result.job_status
                "file_token": "xxx",    ← 注意：是 data.result.file_token
                "file_extension": "pdf",
                "file_name": "xxx",
                "file_size": 12345,
                "job_error_msg": "success"
              }
            }
          }
        不是 data.job.status / data.job.result.file_token ！！
        """
        for i in range(90):  # 最多等 180 秒
            time.sleep(2)

            body = self.call(
                "GET",
                f"/drive/v1/export_tasks/{ticket}",
                params={"token": obj_token},
            )

            # 网络异常 → 继续重试
            if not body:
                if i % 10 == 9:
                    log(f"  轮询无响应 ({i+1}/90)", "WARNING")
                continue

            # 业务错误
            if body.get("code") != 0:
                code, msg = body.get("code"), body.get("msg")
                # 限流 → 等一会重试
                if code in (99991400, 99991401):
                    time.sleep(3)
                    continue
                log(f"  轮询错误: code={code} msg={msg}", "WARNING")
                if i > 5:
                    return None
                continue

            # ── 关键：正确解析响应结构 ──
            result = body.get("data", {}).get("result", {})
            if not result:
                # 某些情况下任务还没生成 result，继续等
                if i % 10 == 0:
                    log(f"  等待任务结果... ({i+1}/90)")
                continue

            job_status = result.get("job_status")

            # job_status == 0 → 导出成功
            if job_status == 0:
                file_token = result.get("file_token")
                if file_token:
                    file_size = result.get("file_size", 0)
                    log(f"  ✅ 导出完成 (file_size={file_size})")
                    return file_token
                log(f"  ❌ 导出成功但无 file_token: {json.dumps(result, ensure_ascii=False)[:300]}", "ERROR")
                return None

            # job_status == 1 或 2 → 处理中
            if job_status in (1, 2):
                if i % 10 == 0:
                    log(f"  ⏳ 处理中... ({i+1}/90)")
                continue

            # 其他状态码 → 各种错误
            error_map = {
                3:   "内部错误",
                107: "文档过大",
                108: "处理超时",
                109: "内容块无权限",
                110: "无权限",
                111: "文档已删除",
                122: "创建副本中禁止导出",
                123: "文档不存在",
                6000: "图片过多",
            }
            err_msg = error_map.get(job_status, result.get("job_error_msg", f"未知错误(code={job_status})"))
            log(f"  ❌ 导出失败: {err_msg}", "ERROR")
            return None

        log(f"  ❌ 导出超时（180秒）", "ERROR")
        return None

    def download_file(self, file_token: str, save_path: Path) -> bool:
        """
        下载导出文件并保存。

        注意：导出文件在任务完成后 10 分钟内有效，必须及时下载。
        如果因 token 过期导致下载失败，会自动刷新 token 重试一次。
        """
        for retry in range(2):  # 最多重试 1 次（处理 token 过期）
            self.ensure_token()
            url = f"{BASE_URL}/drive/v1/export_tasks/file/{file_token}/download"
            headers = {"Authorization": f"Bearer {self.token}"}

            try:
                resp = requests.get(url, headers=headers, stream=True,
                                    allow_redirects=True, timeout=120)

                # token 过期 → 刷新后重试
                if resp.status_code in (401, 403):
                    if retry == 0:
                        log(f"  下载鉴权失败，刷新 token 重试...", "WARNING")
                        self.token = None  # 强制刷新
                        continue
                    resp.raise_for_status()

                resp.raise_for_status()

                # 检查是否返回了错误 JSON 而非文件
                ct = resp.headers.get("Content-Type", "")
                if "application/json" in ct:
                    log(f"  ❌ 下载返回 JSON 而非文件: {resp.text[:300]}", "ERROR")
                    return False

                save_path.parent.mkdir(parents=True, exist_ok=True)
                with open(save_path, "wb") as f:
                    for chunk in resp.iter_content(8192):
                        if chunk:
                            f.write(chunk)

                size = save_path.stat().st_size
                if size == 0:
                    log(f"  ⚠️ 下载文件大小为 0", "WARNING")
                    save_path.unlink()
                    return False

                log(f"  💾 已保存: {save_path} ({size:,} bytes)")
                return True

            except Exception as e:
                log(f"  ❌ 下载失败: {e}", "ERROR")
                if retry == 0 and "401" in str(e) or "403" in str(e):
                    self.token = None
                    continue
                break

        if save_path.exists():
            save_path.unlink()
        return False


# ━━━━━━━━━━━━━━━━━━ 主流程 ━━━━━━━━━━━━━━━━━━


def export_wiki(api: FeishuAPI, export_dir: Path):
    """导出所有知识空间的文档"""

    # 1. 获取知识空间
    spaces = api.list_spaces()
    if not spaces:
        log("未找到任何知识空间", "ERROR")
        return

    log(f"共找到 {len(spaces)} 个知识空间\n")

    stats = {"total": 0, "success": 0, "failed": 0, "skipped": 0}

    for space in spaces:
        space_name = clean_filename(space.get("name", "未命名"))
        space_id = space["space_id"]

        log(f"{'━' * 60}")
        log(f"📁 空间: {space_name} (ID: {space_id})")

        # 2. 获取所有文档节点
        docs = api.list_nodes(space_id)
        log(f"   可导出文档: {len(docs)} 个")
        stats["total"] += len(docs)

        if not docs:
            continue

        for idx, doc in enumerate(docs, 1):
            title = clean_filename(doc["title"])
            _, file_ext = EXPORT_TYPE_MAP[doc["obj_type"]]
            path_parts = [clean_filename(p) for p in doc["path"]]

            # 构建保存路径
            if path_parts:
                save_dir = export_dir / space_name / Path(*path_parts)
            else:
                save_dir = export_dir / space_name
            save_path = save_dir / f"{title}.{file_ext}"

            # 已存在 → 跳过
            if save_path.exists():
                log(f"  [{idx}/{len(docs)}] ⏭ 已存在: {title}")
                stats["skipped"] += 1
                continue

            log(f"  [{idx}/{len(docs)}] 📄 {title} (type={doc['obj_type']})")

            # 3. 创建导出任务
            ticket = api.create_export_task(doc["obj_token"], doc["obj_type"])
            if not ticket:
                stats["failed"] += 1
                continue

            # 4. 轮询导出结果
            file_token = api.poll_export_task(ticket, doc["obj_token"])
            if not file_token:
                stats["failed"] += 1
                continue

            # 5. 下载文件
            if api.download_file(file_token, save_path):
                stats["success"] += 1
            else:
                stats["failed"] += 1

            # 间隔，避免触发限流
            time.sleep(0.3)

    # 统计
    log(f"\n{'━' * 60}")
    log(f"导出完成！共 {stats['total']} 个文档")
    log(f"  ✅ 成功: {stats['success']}")
    log(f"  ❌ 失败: {stats['failed']}")
    log(f"  ⏭ 跳过: {stats['skipped']}")
    log(f"{'━' * 60}")


def main():
    if not APP_ID or not APP_SECRET:
        print("请检查 APP_ID 和 APP_SECRET 是否已配置")
        sys.exit(1)

    export_dir = Path(EXPORT_DIR)
    export_dir.mkdir(parents=True, exist_ok=True)

    log(f"导出目录: {export_dir.resolve()}")

    api = FeishuAPI(APP_ID, APP_SECRET)
    export_wiki(api, export_dir)


if __name__ == "__main__":
    main()