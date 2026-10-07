# -*- coding: utf-8 -*-
# client.py — WebSocket 客户端 & QQ API 调用（BotClient、鉴权、发消息、群禁言、WS连接）
import json
import time
import asyncio
import hashlib
import requests
import websockets
import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List
from urllib.parse import urlparse

from config import (get_executor, get_bot_name, set_bot_name, get_bots,
                    get_bot_enabled, get_media_block, get_enable_chunked_upload,
                    get_chunk_upload_download_retries,
                    MEDIA_CACHE_DIR)
from log import info, warn, error, debug, LogCtx
from utils import infer_file_type, needs_dual_send


# ==================== 大文件分块上传（URL 直传失败后的兜底通道） ====================
# 官方 URL 直传（/files 带 url 字段）要求 QQ 服务端能自己去拉那个 URL，
# 对防盗链、需要鉴权、内网地址、以及大文件都容易失败。兜底方案是：
#   本机把文件下载下来 → 走官方分片上传 → 拿到 file_info。
# 分片上传三步（单聊用 /v2/users/...，群聊用 /v2/groups/...，路径仅前缀不同）：
#   1) POST /v2/{users|groups}/{openid}/upload_prepare   取 upload_id + 分片预签名地址
#   2) PUT  <presigned_url> 传数据；再 POST /upload_part_finish 上报该片
#   3) POST /v2/{users|groups}/{openid}/files           带 upload_id 换取 file_info
#
# 仅当直传失败且文件不超过 CHUNK_UPLOAD_MAX_SIZE 时才走这条路，避免为了
# 一个几百 MB 的文件白下载一场。

# 触发下载重试的大小上限（200MB）。超过此值不下载，直接判定失败。
CHUNK_UPLOAD_MAX_SIZE = 200 * 1024 * 1024

# 探测远端大小时的超时（秒）。HEAD 被拒时会退化为 GET 流式读首块。
_REMOTE_SIZE_PROBE_TIMEOUT = 20

# 下载整体超时（秒）与单次读块大小
_DOWNLOAD_TIMEOUT = 600
_DOWNLOAD_CHUNK = 1024 * 1024

# 分片 PUT / finish 的重试次数
_CHUNK_RETRIES = 3

# 下载网络失败的重试次数现由配置项 CHUNK_UPLOAD_DOWNLOAD_RETRIES 实时决定
# （见 config.get_chunk_upload_download_retries，默认 2，范围 [1,10]）。
# 仅对可重试的失败生效：网络中断/超时/5xx 会重试；
# 超过大小上限与 HTTP 4xx 不重试（重试也不会变好）。
# 调用点：_download_to_temp —— 每次都重新读配置，改完即生效。

# 临时文件目录（分块上传需要真实文件落盘）。放在 media_cache 下，
# 与项目其他缓存同级，便于运维统一清理。
UPLOAD_TMP_DIR = MEDIA_CACHE_DIR / "_upload_tmp"


class _DownloadTooLarge(Exception):
    """下载过程中发现文件超过上限。用于把「超限」从下载线程里抛出来，
    与普通网络异常区分开 —— 两者都需要删除临时文件，但日志语义不同。"""

    def __init__(self, size: int):
        super().__init__(f"文件超过上限（已下载 {size} 字节）")
        self.size = size


def _fmt_mb(n: int) -> str:
    """把字节数格式化成 MB 字符串，仅用于日志。"""
    try:
        return f"{int(n) / 1024 / 1024:.2f}MB"
    except Exception:
        return "?"


def _probe_remote_size(url: str) -> Optional[int]:
    """探测远端文件大小（字节）。拿不到返回 None（表示"未知"，不是"太大"）。

    先试 HEAD（大多数站点会返回 Content-Length）；被拒或没有该头时，
    退化为 GET 流式只读首块、从响应头里取 Content-Length。

    注意：这里**不下载正文**（HEAD 无正文；GET 只读一次就关闭），
    因此即便目标是几百 MB 也不会产生实际流量。
    """
    try:
        r = requests.head(url, timeout=_REMOTE_SIZE_PROBE_TIMEOUT, allow_redirects=True)
        if r.status_code < 400:
            cl = r.headers.get("Content-Length")
            if cl and str(cl).isdigit():
                return int(cl)
    except Exception:
        pass

    # HEAD 不可用（405/403/无 Content-Length）→ 用 GET 流式拿响应头
    try:
        with requests.get(url, timeout=_REMOTE_SIZE_PROBE_TIMEOUT,
                          stream=True, allow_redirects=True) as r:
            cl = r.headers.get("Content-Length")
            if cl and str(cl).isdigit():
                return int(cl)
    except Exception:
        pass
    return None


def _sha1_and_md5(path: Path) -> Tuple[str, str, str]:
    """流式计算文件的 md5 / sha1 / md5_10m，返回 (md5, sha1, md5_10m)。

    md5_10m 是官方要求的前 10,002,432 字节的 md5（不是 10MB=10485760）。
    分块读取（8MB/次），避免大文件一次性读进内存。
    """
    MD5_10M_SIZE = 10_002_432
    md5 = hashlib.md5()
    sha1 = hashlib.sha1()
    md5_10m = hashlib.md5()
    read_10m = 0
    with open(path, "rb") as f:
        while True:
            buf = f.read(8 * 1024 * 1024)
            if not buf:
                break
            md5.update(buf)
            sha1.update(buf)
            if read_10m < MD5_10M_SIZE:
                take = min(len(buf), MD5_10M_SIZE - read_10m)
                md5_10m.update(buf[:take])
                read_10m += take
    return md5.hexdigest(), sha1.hexdigest(), md5_10m.hexdigest()


# 正在处理的消息任务集合（持有引用，防止任务被 GC 提前回收）。
# 见 ws 接收循环：消息处理改为 create_task 派发，不再阻塞收包。
_message_tasks: set = set()

# 长期后台任务（心跳等）的强引用集合。
# create_task 的返回值若不持有，任务可能在运行中被 GC 回收 —— 心跳一旦
# 被回收，连接就不会再发 op=1，服务端会在心跳超时后判定掉线并断开。
_background_tasks: set = set()


def _derive_media_name(source: str, file_name: Optional[str] = None) -> str:
    """推导媒体文件名，规则固定为：**传入优先，其次从 URL 推导，最后兜底**。

    三级回退：
      1) file_name  —— 调用方显式传入的名字，优先级最高，有值就一定用它，
                       **不会被 URL 里的名字覆盖**（本例最容易踩的坑）；
      2) URL 路径的最后一段 —— urlparse 取 path，自动剥离 ?query；
      3) "media"   —— 前两者都拿不到时的兜底，保证返回值永不为空。

    细节与理由：
      - 空串 / 纯空白视为「没传」：调用方常用 "" 表示未指定，
        因此这里先 strip 再判真值，与既有的 `file_name or ...` 行为一致；
      - 传 "  x.bin  " 这类带空白的名字会被 strip，避免生成带空格的文件名；
      - URL 以 / 结尾（如 https://x.com/a/b/）时 Path().name 得 "b" 而非空，
        名字不好看但可用；
      - 只有域名或根路径（https://x.com、https://x.com/）时 path 为 "" 或 "/",
        落到兜底 "media"。

    返回值保证是非空字符串，调用方无需再判空。
    """
    # ---- 1. 传入优先 ----
    name = ""
    try:
        name = str(file_name).strip() if file_name else ""
    except Exception:
        name = ""
    if name:
        return name

    # ---- 2. 从 URL 推导（去掉查询串，只取路径最后一段）----
    try:
        derived = Path(urlparse(source or "").path).name
        if derived:
            return derived
    except Exception:
        pass

    # ---- 3. 兜底 ----
    return "media"


def _spawn_background(coro, name: str, ctx=None):
    """把协程派发为长期后台任务，并持有强引用直到它结束。

    与 _message_tasks 分开管理：那些是短命的消息处理任务，这些是随连接
    存活的常驻任务。两者都用 add_done_callback 在结束时移除引用，
    同时**读取一次异常** —— 只 discard 而不取异常，任务内的报错会以
    "Task exception was never retrieved" 的形式丢到 stderr，
    既不进日志也不易排查。
    """
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)

    def _done(t: asyncio.Task):
        _background_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            error(f"[后台任务 {name}] 异常退出: {type(exc).__name__}: {exc}", ctx=ctx)

    task.add_done_callback(_done)
    return task


def _log_message_task_result(t: asyncio.Task):
    """消息处理任务结束时的回调：释放引用 + 取出异常写进日志。

    取消（CancelledError）是正常路径 —— 同会话新消息到达时会取消上一条的
    处理任务，因此不算错误、不记 error。
    """
    _message_tasks.discard(t)
    if t.cancelled():
        return
    exc = t.exception()
    if exc is not None:
        error(f"[处理消息错误] 任务异常退出: {type(exc).__name__}: {exc}", ctx=None)


# ==================== BotClient：Token 管理 & QQ API 调用 ====================
class BotClient:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret
        self.token_info = {"access_token": None, "expires_at": 0}
        self.bot_name = "蓝狼"
        # 用于消息去重的 msg_seq 计数器
        self._msg_seq_map = {}  # key: msg_id, value: int
        # 最近一次发送消息的响应元数据，供外部读取
        self._last_send_meta: Dict[str, Any] = {}
        # 本实例发送过的全部消息（按发送顺序），供一次调用产生多条消息时读取。
        # 与 _last_send_meta 的区别：后者只保留最后一次，双发场景会覆盖前一条。
        self.sent_messages: List[Dict[str, Any]] = []
        # 上次 send_message 调用产生的消息（每次调用前清空，只含本次）
        self._call_sent: List[Dict[str, Any]] = []

    def begin_send_batch(self):
        """开始一次发送批次，重置本次调用的消息记录。"""
        self._call_sent = []
        return self._call_sent

    def get_call_sent(self) -> List[Dict[str, Any]]:
        """获取上一次 send_message 调用产生的全部消息 [{"msg_id","msg_idx"}]。

        注意：send_with_policy 会连续调用多次 send_message，需配合
        begin_send_batch() 在每次调用前重置，否则只反映最后一次。
        """
        return list(self._call_sent)

    def get_access_token(self, force_refresh: bool = False) -> str:
        ctx = LogCtx(app_id=self.app_id)
        if force_refresh or not self.token_info["access_token"] or time.time() >= self.token_info["expires_at"] - 60:
            url = "https://bots.qq.com/app/getAppAccessToken"
            headers = {"Content-Type": "application/json"}
            payload = {"appId": self.app_id, "clientSecret": self.app_secret}
            resp = requests.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            data = resp.json()
            self.token_info["access_token"] = data["access_token"]
            self.token_info["expires_at"] = time.time() + int(data["expires_in"])
            info(f"[Token] {self.app_id} 获取成功，有效期 {data['expires_in']} 秒", ctx=ctx)
        return self.token_info["access_token"]

    def get_websocket_url(self) -> str:
        token = self.get_access_token()
        url = "https://api.sgroup.qq.com/gateway"
        headers = {"Authorization": f"QQBot {token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()["url"]

    # ========== 异步包装（避免阻塞事件循环） ==========
    # get_access_token / get_websocket_url 内部用的是**同步 requests**，
    # 直接从协程里调用会把整个事件循环卡住（本项目所有机器人在同一个
    # 循环上跑）：取 token 与取网关地址都是跨公网请求，一旦网络抖动，
    # 期间所有机器人的收包、定时任务、API 请求全部停摆。
    # 因此协程路径一律改用下面两个包装 —— 把阻塞调用丢进
    # config._executor 线程池，与项目里其它 requests 调用保持一致。
    async def get_access_token_async(self, force_refresh: bool = False) -> str:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            get_executor(), lambda: self.get_access_token(force_refresh)
        )

    async def get_websocket_url_async(self) -> str:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(get_executor(), self.get_websocket_url)

    # ========== 媒体上传（仅支持 URL 直接上传） ==========
    async def _download_to_temp(self, source: str, file_name: Optional[str],
                                ctx: LogCtx) -> Tuple[Optional[Path], Optional[str]]:
        """把远端文件下载到临时目录，返回 (临时文件路径, 错误信息)。

        网络中断等**可重试**失败会按配置 CHUNK_UPLOAD_DOWNLOAD_RETRIES
        （实时读取，默认 2，含首次在内）重试；每次尝试都是全新的临时文件，
        不会把两次的字节拼在一起。

        以下两类**不重试**（重试没有意义，只会浪费时间/流量）：
          - 文件超过上限（_DownloadTooLarge）：内容本身就不合规；
          - HTTP 4xx（如 404/403）：URL 或权限问题，重试结果一样。

        成功时调用方**必须**负责删除返回的临时文件（见 upload_media_chunked
        的 finally）。失败返回 (None, 原因)。
        """
        loop = asyncio.get_event_loop()
        try:
            UPLOAD_TMP_DIR.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            return None, f"无法创建临时目录 {UPLOAD_TMP_DIR}: {e}"

        # 文件名保留原扩展名，便于服务端后续按名字识别类型。
        # 走共用的 _derive_media_name（传入优先 → URL 推导 → media），
        # 保证与下面 upload_media_chunked 里给 QQ 用的名字同源、规则一致。
        suffix = ""
        try:
            suffix = Path(_derive_media_name(source, file_name)).suffix
        except Exception:
            suffix = ""

        def _attempt() -> Path:
            """单次下载尝试。失败会删掉自己的临时文件再抛出。"""
            # delete=False：句柄关闭后文件仍需存在，由调用方删除
            fd, tmp_name = tempfile.mkstemp(prefix="qqup_", suffix=suffix,
                                            dir=str(UPLOAD_TMP_DIR))
            os.close(fd)
            tmp_path = Path(tmp_name)
            total = 0
            try:
                with requests.get(source, timeout=_DOWNLOAD_TIMEOUT,
                                  stream=True, allow_redirects=True) as r:
                    r.raise_for_status()

                    # 若响应头就带了 Content-Length 且已超限，连第一个字节都不用写
                    cl = r.headers.get("Content-Length")
                    if cl and str(cl).isdigit() and int(cl) > CHUNK_UPLOAD_MAX_SIZE:
                        raise _DownloadTooLarge(int(cl))

                    with open(tmp_path, "wb") as f:
                        for block in r.iter_content(chunk_size=_DOWNLOAD_CHUNK):
                            if not block:
                                continue
                            total += len(block)
                            # ★ 边下边判：拿不到 Content-Length（分块传输、
                            #   动态生成等）时只能靠这里兜住，一旦超限立刻
                            #   停止下载，不再继续拉取剩余字节。
                            if total > CHUNK_UPLOAD_MAX_SIZE:
                                raise _DownloadTooLarge(total)
                            f.write(block)
            except BaseException:
                # 任何失败（含超限、网络中断）都要删掉半截文件，
                # 否则 media_cache/_upload_tmp 会堆积垃圾。
                # 每次尝试各自清理，重试不会留下上一次的残骸。
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
                raise
            return tmp_path

        def _is_retryable(exc: Exception) -> bool:
            """判断该异常是否值得重试。"""
            # 超限不重试：内容本身超规，重试还是超
            if isinstance(exc, _DownloadTooLarge):
                return False
            # HTTP 4xx 不重试（URL 错误/无权限），5xx 与网络异常可重试
            if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
                code = exc.response.status_code
                return not (400 <= code < 500)
            # 其余（连接中断、超时、读流中断等）一律视为可重试
            return True

        last_exc: Optional[Exception] = None
        # 实时读取重试次数（默认 2，范围 [1,10]）：每次下载都重新读配置，
        # 改完即生效，无需重启。读失败时回退 2，绝不因为配置异常而放弃下载。
        try:
            max_attempts = get_chunk_upload_download_retries()
        except Exception:
            max_attempts = 2

        for attempt in range(max_attempts):
            try:
                tmp_path = await loop.run_in_executor(get_executor(), _attempt)
            except _DownloadTooLarge as e:
                warn(f"[分块上传] 下载中已超过上限（{_fmt_mb(e.size)} > "
                     f"{_fmt_mb(CHUNK_UPLOAD_MAX_SIZE)}），已停止下载并删除临时文件", ctx=ctx)
                return None, f"文件超过 {_fmt_mb(CHUNK_UPLOAD_MAX_SIZE)} 上限"
            except Exception as e:
                last_exc = e
                # 不可重试的失败直接返回，不做无谓的重试
                if not _is_retryable(e):
                    error(f"[分块上传] 下载失败（不可重试）: {e}", ctx=ctx)
                    return None, f"下载失败: {e}"
                if attempt < max_attempts - 1:
                    warn(f"[分块上传] 下载失败（{type(e).__name__}: {e}），"
                         f"重试 {attempt + 1}/{max_attempts - 1}", ctx=ctx)
                    await asyncio.sleep(2 * (attempt + 1))  # 递增退避
                    continue
                error(f"[分块上传] 下载失败，已重试 {max_attempts} 次: {e}", ctx=ctx)
                return None, f"下载失败: {e}"
            else:
                break

        if tmp_path is None:
            # 理论上到不了这里（循环内每条失败路径都已 return/break），
            # 保留兜底以防将来改动遗漏
            return None, f"下载失败: {last_exc}"

        try:
            size = tmp_path.stat().st_size
        except Exception as e:
            return None, f"下载后无法读取文件: {e}"

        if size <= 0:
            # 空文件同样上传不了，且会让后续 md5 计算失去意义
            try:
                tmp_path.unlink()
            except Exception:
                pass
            return None, "下载得到的文件为空"

        info(f"[分块上传] 下载完成: {_fmt_mb(size)} -> {tmp_path.name}", ctx=ctx)
        return tmp_path, None

    async def _upload_prepare(self, endpoint: str, recipient_id: str, file_size: int,
                              file_name: str, hashes: Dict[str, str],
                              file_type: int, ctx: LogCtx) -> Optional[Dict]:
        """Step 1：申请分片上传，返回 prepare 响应（含 upload_id 与分片地址）。"""
        token = await self.get_access_token_async()
        url = f"https://api.sgroup.qq.com/v2/{endpoint}/{recipient_id}/upload_prepare"
        headers = {"Authorization": f"QQBot {token}", "Content-Type": "application/json"}
        payload = {
            "file_type": file_type,
            # 官方要求 file_size 为字符串
            "file_size": str(file_size),
            "file_name": file_name,
            "md5": hashes["md5"],
            "sha1": hashes["sha1"],
            "md5_10m": hashes["md5_10m"],
        }
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.post(url, json=payload, headers=headers, timeout=60)
            )
        except Exception as e:
            error(f"[分块上传] upload_prepare 请求异常: {e}", ctx=ctx)
            return None
        if resp.status_code != 200:
            error(f"[分块上传] upload_prepare 失败，状态码 {resp.status_code}，"
                  f"响应: {resp.text[:200]}", ctx=ctx)
            return None
        try:
            data = resp.json()
        except Exception as e:
            error(f"[分块上传] upload_prepare 响应非 JSON: {e}", ctx=ctx)
            return None
        if not data.get("upload_id"):
            error(f"[分块上传] upload_prepare 响应缺少 upload_id: {str(data)[:200]}", ctx=ctx)
            return None
        return data

    async def _put_part_with_retry(self, presigned_url: str, data: bytes,
                                   ctx: LogCtx) -> bool:
        """PUT 分片数据到预签名地址（带重试）。成功返回 True。"""
        loop = asyncio.get_event_loop()
        for attempt in range(_CHUNK_RETRIES):
            try:
                # 关键：不带 Authorization —— COS 预签名 URL 自带鉴权，
                # 额外头会导致签名不匹配。
                resp = await loop.run_in_executor(
                    get_executor(),
                    lambda: requests.put(presigned_url, data=data, timeout=300)
                )
                if resp.status_code < 400:
                    return True
                warn(f"[分块上传] 分片 PUT 返回 {resp.status_code}，"
                     f"重试 {attempt + 1}/{_CHUNK_RETRIES}", ctx=ctx)
            except Exception as e:
                warn(f"[分块上传] 分片 PUT 异常: {e}，重试 {attempt + 1}/{_CHUNK_RETRIES}", ctx=ctx)
            if attempt < _CHUNK_RETRIES - 1:
                await asyncio.sleep(2)
        return False

    async def _finish_part_with_retry(self, endpoint: str, recipient_id: str,
                                      upload_id: str, idx: int, block_size: int,
                                      md5: str, ctx: LogCtx) -> bool:
        """通知服务端某分片已完成（带重试）。成功返回 True。"""
        token = await self.get_access_token_async()
        url = f"https://api.sgroup.qq.com/v2/{endpoint}/{recipient_id}/upload_part_finish"
        headers = {"Authorization": f"QQBot {token}", "Content-Type": "application/json"}
        payload = {
            "upload_id": upload_id,
            "part_index": idx,
            "block_size": str(block_size),
            "md5": md5,
        }
        loop = asyncio.get_event_loop()
        for attempt in range(_CHUNK_RETRIES):
            try:
                resp = await loop.run_in_executor(
                    get_executor(),
                    lambda: requests.post(url, json=payload, headers=headers, timeout=60)
                )
                if resp.status_code < 400:
                    return True
                warn(f"[分块上传] 分片 finish 返回 {resp.status_code}: {resp.text[:150]}，"
                     f"重试 {attempt + 1}/{_CHUNK_RETRIES}", ctx=ctx)
            except Exception as e:
                warn(f"[分块上传] 分片 finish 异常: {e}，重试 {attempt + 1}/{_CHUNK_RETRIES}", ctx=ctx)
            if attempt < _CHUNK_RETRIES - 1:
                await asyncio.sleep(2)
        return False

    async def _upload_file_chunked(self, endpoint: str, recipient_id: str, path: Path,
                                   file_name: str, file_type: int,
                                   ctx: LogCtx) -> Optional[str]:
        """把一个本地文件走官方分片上传，成功返回 file_info。

        流程：upload_prepare → 逐片 (PUT + upload_part_finish) → /files 换取 file_info。
        任一步失败返回 None（调用方按整体失败处理）。
        """
        loop = asyncio.get_event_loop()

        try:
            file_size = path.stat().st_size
        except Exception as e:
            error(f"[分块上传] 无法读取文件大小: {e}", ctx=ctx)
            return None

        # 计算三个校验值（官方 upload_prepare 必填）
        try:
            md5, sha1, md5_10m = await loop.run_in_executor(
                get_executor(), lambda: _sha1_and_md5(path)
            )
        except Exception as e:
            error(f"[分块上传] 计算校验值失败: {e}", ctx=ctx)
            return None

        hashes = {"md5": md5, "sha1": sha1, "md5_10m": md5_10m}
        info(f"[分块上传] 文件 {_fmt_mb(file_size)}，md5={md5[:16]}...", ctx=ctx)

        # ---- Step 1 ----
        prepare = await self._upload_prepare(endpoint, recipient_id, file_size,
                                             file_name, hashes, file_type, ctx)
        if not prepare:
            return None

        upload_id = prepare["upload_id"]
        try:
            block_size = int(prepare.get("block_size") or 0)
        except Exception:
            block_size = 0

        # 兼容 parts / part_list 两种字段名
        parts = None
        for key in ("parts", "part_list"):
            if isinstance(prepare.get(key), list):
                parts = prepare[key]
                break
        if not parts:
            error(f"[分块上传] 响应中找不到分片列表，字段: {list(prepare.keys())}", ctx=ctx)
            return None
        if block_size <= 0:
            error(f"[分块上传] 响应中 block_size 无效: {prepare.get('block_size')}", ctx=ctx)
            return None

        total = len(parts)
        info(f"[分块上传] upload_id={upload_id}，block_size={_fmt_mb(block_size)}，"
             f"共 {total} 片", ctx=ctx)

        # ---- Step 2：逐片上传 ----
        try:
            with open(path, "rb") as f:
                for i, part in enumerate(parts):
                    if not isinstance(part, dict):
                        error(f"[分块上传] 第 {i + 1} 个分片不是对象: {part!r}", ctx=ctx)
                        return None
                    # 官方 index 从 1 开始；缺失时按顺序回退
                    try:
                        idx = int(part.get("index", i + 1))
                    except Exception:
                        idx = i + 1
                    presigned_url = part.get("presigned_url") or part.get("url")
                    if not presigned_url:
                        error(f"[分块上传] 分片 {idx} 缺少上传地址，字段: {list(part.keys())}",
                              ctx=ctx)
                        return None

                    # index 从 1 开始 → 文件偏移量用 (idx - 1)
                    f.seek((idx - 1) * block_size)
                    chunk = f.read(block_size)
                    if not chunk:
                        # 分片数超出文件实际大小：服务端给的列表与文件不匹配
                        error(f"[分块上传] 分片 {idx} 读取到空数据"
                              f"（文件可能小于服务端预期）", ctx=ctx)
                        return None
                    chunk_md5 = hashlib.md5(chunk).hexdigest()

                    if not await self._put_part_with_retry(presigned_url, chunk, ctx):
                        error(f"[分块上传] 分片 {idx}/{total} 上传失败", ctx=ctx)
                        return None
                    if not await self._finish_part_with_retry(endpoint, recipient_id, upload_id,
                                                              idx, len(chunk), chunk_md5, ctx):
                        error(f"[分块上传] 分片 {idx}/{total} 上报失败", ctx=ctx)
                        return None

                    if idx % 10 == 0 or idx == total:
                        info(f"[分块上传] 进度 {idx}/{total}", ctx=ctx)
        except Exception as e:
            error(f"[分块上传] 分片上传异常: {e}", ctx=ctx)
            return None

        # ---- Step 3：换取 file_info ----
        token = await self.get_access_token_async()
        url = f"https://api.sgroup.qq.com/v2/{endpoint}/{recipient_id}/files"
        headers = {"Authorization": f"QQBot {token}", "Content-Type": "application/json"}
        payload = {
            "file_type": file_type,
            "file_name": file_name,
            "upload_id": upload_id,
            "srv_send_msg": False,
        }
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.post(url, json=payload, headers=headers, timeout=60)
            )
        except Exception as e:
            error(f"[分块上传] 换取 file_info 请求异常: {e}", ctx=ctx)
            return None
        if resp.status_code != 200:
            error(f"[分块上传] 换取 file_info 失败，状态码 {resp.status_code}，"
                  f"响应: {resp.text[:200]}", ctx=ctx)
            return None
        try:
            file_info = (resp.json() or {}).get("file_info")
        except Exception as e:
            error(f"[分块上传] 换取 file_info 响应异常: {e}", ctx=ctx)
            return None
        if not file_info:
            error(f"[分块上传] 响应缺少 file_info: {resp.text[:200]}", ctx=ctx)
            return None
        info(f"[分块上传] 成功，file_info: {file_info[:20]}...", ctx=ctx)
        return file_info

    async def upload_media_chunked(self, msg_type: str, recipient_id: str, source: str,
                                   file_type: Optional[int] = None,
                                   file_name: Optional[str] = None) -> Optional[str]:
        """下载远端文件后走分片上传，返回 file_info；失败返回 None。

        这是 URL 直传失败后的兜底通道，调用方见 upload_media_by_url。
        仅当文件不超过 CHUNK_UPLOAD_MAX_SIZE 时才会真正下载。
        """
        ctx = LogCtx(app_id=self.app_id,
                     thread_key=f"{'c2c' if msg_type == 'c2c' else 'group'}_{recipient_id}")

        if file_type is None:
            file_type = infer_file_type(file_name, source)

        # ---------- 1. 先探大小：过大就不下载 ----------
        size = await asyncio.get_event_loop().run_in_executor(
            get_executor(), lambda: _probe_remote_size(source)
        )
        if size is not None and size > CHUNK_UPLOAD_MAX_SIZE:
            warn(f"[分块上传] 文件 {_fmt_mb(size)} 超过上限 "
                 f"{_fmt_mb(CHUNK_UPLOAD_MAX_SIZE)}，放弃下载重试", ctx=ctx)
            return None
        if size is None:
            # 拿不到大小（站点不支持 HEAD、分块传输等）→ 保守尝试下载。
            # 此时由 _download_to_temp 边下边判：一旦超过上限立刻停止并删除，
            # 不会把超大文件完整拖到本地。
            info("[分块上传] 无法探测文件大小，保守尝试下载（超限会立即中止）", ctx=ctx)
        else:
            info(f"[分块上传] 探测到文件大小 {_fmt_mb(size)}，开始下载重试", ctx=ctx)

        tmp_path = None
        try:
            # ---------- 2. 下载到临时文件（超限会中途停止并删除） ----------
            tmp_path, err = await self._download_to_temp(source, file_name, ctx)
            if not tmp_path:
                warn(f"[分块上传] {err}", ctx=ctx)
                return None

            # 兜底复核：正常路径下 _download_to_temp 已在下载途中卡住上限，
            # 这里再核一次实际落盘大小，防止「边下边判」被绕过（例如未来
            # 改动下载实现、或文件系统报告的大小与写入量不一致）。
            try:
                actual = tmp_path.stat().st_size
            except Exception:
                actual = 0
            if actual > CHUNK_UPLOAD_MAX_SIZE:
                warn(f"[分块上传] 实际下载到 {_fmt_mb(actual)} 超过上限，放弃", ctx=ctx)
                return None

            # ---------- 3. 分片上传 ----------
            endpoint = "users" if msg_type == "c2c" else "groups"
            # 上传用的文件名：传入优先，其次从 URL 推导，最后 media
            # （规则集中在 _derive_media_name，返回值保证非空）
            upload_name = _derive_media_name(source, file_name)
            return await self._upload_file_chunked(endpoint, recipient_id, tmp_path,
                                                   upload_name, file_type, ctx)
        finally:
            # 无论成败都清理临时文件，避免 media_cache/_upload_tmp 无限增长
            if tmp_path is not None:
                try:
                    tmp_path.unlink()
                    debug(f"[分块上传] 已清理临时文件 {tmp_path.name}", ctx=ctx)
                except Exception as e:
                    warn(f"[分块上传] 临时文件清理失败 {tmp_path}: {e}", ctx=ctx)

    async def _fallback_chunked(self, msg_type: str, recipient_id: str, source: str,
                                file_type: Optional[int], file_name: Optional[str],
                                ctx: LogCtx, reason: str) -> Optional[str]:
        """直传失败后的统一兜底入口：按开关决定是否走「下载 + 分片上传」。

        开关 ENABLE_CHUNKED_UPLOAD 实时读取，改完即生效（无需重启）：
          1 = 开启 → 调 upload_media_chunked（内部还会判 200MB 上限）
          0 = 关闭 → 直接返回 None，不下载任何内容

        把开关判断收在这一处，是为了三个失败分支（请求异常 / 非 200 / 无
        file_info）行为完全一致，不会出现某个分支漏判开关的情况。
        """
        if not get_enable_chunked_upload():
            warn(f"[上传URL] {reason}；分块上传兜底已关闭"
                 f"（ENABLE_CHUNKED_UPLOAD=0），放弃", ctx=ctx)
            return None
        warn(f"[上传URL] {reason}；转分块上传重试...", ctx=ctx)
        return await self.upload_media_chunked(msg_type, recipient_id, source,
                                               file_type, file_name)

    # ========== 媒体上传（URL 直传，失败后回退分块上传） ==========
    async def upload_media_by_url(self, msg_type: str, recipient_id: str,
                                  source: str, file_type: Optional[int] = None,
                                  file_name: Optional[str] = None) -> Optional[str]:
        """
        使用官方 /files 接口直接传入 URL 上传。
        直传失败时，若文件不超过 CHUNK_UPLOAD_MAX_SIZE，自动回退为
        「先下载再分块上传」（见 upload_media_chunked）。
        返回 file_info，失败返回 None。
        """
    async def upload_media_by_url(self, msg_type: str, recipient_id: str,
                                  source: str, file_type: Optional[int] = None,
                                  file_name: Optional[str] = None) -> Optional[str]:
        """
        使用官方 /files 接口直接传入 URL 上传。
        返回 file_info，失败返回 None。
        """
        # 日志标识：app_id + 会话（msg_type=group_xxx / c2c_xxx）
        ctx = LogCtx(app_id=self.app_id,
                     thread_key=f"{'c2c' if msg_type == 'c2c' else 'group'}_{recipient_id}")
        info(f"[上传URL] 收到 file_name: {file_name}, file_type: {file_type}", ctx=ctx)
        if not (source.startswith('http://') or source.startswith('https://')):
            warn(f"[上传URL] 错误：仅支持 HTTP/HTTPS URL，收到: {source[:80]}", ctx=ctx)
            return None

        # 确定 file_type（若未指定则自动推断）；实现见 utils.infer_file_type
        if file_type is None:
            file_type = infer_file_type(file_name, source)

        token = await self.get_access_token_async()
        endpoint = "users" if msg_type == "c2c" else "groups"
        url = f"https://api.sgroup.qq.com/v2/{endpoint}/{recipient_id}/files"
        headers = {
            "Authorization": f"QQBot {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "file_type": file_type,
            "url": source,
            "srv_send_msg": False
        }
        if file_name:
            payload["file_name"] = file_name

        info(f"[上传URL] 类型 {file_type}，目标 {source[:80]}...", ctx=ctx)
        debug(f"[上传URL] 完整 payload: {json.dumps(payload, ensure_ascii=False)}", ctx=ctx)
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.post(url, json=payload, headers=headers, timeout=30)
            )
        except Exception as e:
            error(f"[上传URL] 请求异常: {e}", ctx=ctx)
            return await self._fallback_chunked(msg_type, recipient_id, source,
                                                file_type, file_name, ctx,
                                                "直传请求异常")

        if resp.status_code != 200:
            error(f"[上传URL] 失败，状态码 {resp.status_code}，响应: {resp.text[:200]}", ctx=ctx)
            # 直传失败 → 兜底走「下载 + 分块上传」（超限的文件会在其中直接放弃）
            return await self._fallback_chunked(msg_type, recipient_id, source,
                                                file_type, file_name, ctx,
                                                f"直传失败（HTTP {resp.status_code}）")

        resp_json = resp.json()
        file_info = resp_json.get('file_info')
        if not file_info:
            warn(f"[上传URL] 响应无 file_info: {resp_json}", ctx=ctx)
            # 直传"成功"但拿不到 file_info，同样视为失败并兜底
            return await self._fallback_chunked(msg_type, recipient_id, source,
                                                file_type, file_name, ctx,
                                                "直传响应无 file_info")
        info(f"[上传URL] 成功，file_info: {file_info[:20]}...", ctx=ctx)
        return file_info

    def _parse_send_response(self, resp) -> Optional[str]:
        """从API响应中解析消息ID，同时存储 ext_info（如 msg_idx = ref_idx）到 self._last_send_meta。
        ref_idx 是这条发送消息自己在系统中的索引，其他消息引用它时会用到这个值。
        返回消息ID字符串，失败返回None。"""
        try:
            resp_json = resp.json()
            msg_id_resp = resp_json.get("id") or resp_json.get("message_id")
            # 存储完整的发送响应元数据
            ext_info = resp_json.get("ext_info") or {}
            ref_idx = ext_info.get("ref_idx") if isinstance(ext_info, dict) else None
            self._last_send_meta = {
                "id": msg_id_resp,
                "msg_idx": ref_idx,  # ref_idx = 本消息的 msg_idx
                "ref_idx": ref_idx,  # 保留旧字段名兼容
            }
            # 追加到本实例的发送记录，使"一次调用产生多条消息"可被完整读取
            if msg_id_resp:
                entry = {"msg_id": msg_id_resp, "msg_idx": ref_idx}
                self.sent_messages.append(entry)
                self._call_sent.append(entry)
            return msg_id_resp if msg_id_resp else None
        except Exception:
            self._last_send_meta = {}
            return None

    def get_last_send_msg_idx(self) -> Optional[str]:
        """获取最近一次发送消息的 msg_idx（其他消息引用本消息时使用的索引）。"""
        return self._last_send_meta.get("msg_idx")

    def get_last_send_ref_idx(self) -> Optional[str]:
        """获取最近一次发送消息的 ref_idx（等同于 msg_idx，为兼容保留）。"""
        return self._last_send_meta.get("ref_idx")

    def get_last_send_id(self) -> Optional[str]:
        """获取最近一次发送消息的 id。"""
        return self._last_send_meta.get("id")

    # ========== 发送消息（支持 msg_seq 自动去重） ==========
    async def send_message(self, msg_type: str, recipient_id: str,
                           content: Optional[str] = None,
                           msg_id: Optional[str] = None,
                           media: Optional[str] = None,
                           media_source: Optional[str] = None,
                           embed: Optional[Dict] = None,
                           file_type: Optional[int] = None,
                           file_name: Optional[str] = None,
                           msg_seq: Optional[int] = None,   # 新增可选参数
                           markdown: Optional[Any] = None,  # Markdown 消息（msg_type=2）
                           quote_msg_idx: Optional[str] = None,  # 引用回复：被引用消息的 msg_idx
                           max_retries: int = 3) -> Optional[str]:
        """
        发送消息，支持文本、富媒体、Markdown。
        media_source 必须是 HTTP/HTTPS URL（不支持本地文件）。
        若上传失败，直接返回 None，无自动回退。
        如果提供了 msg_id 且未提供 msg_seq，则自动分配递增序列号。

        markdown 参数支持三种写法（优先级高于 content）：
          - str：自定义 markdown 文本 → {"markdown": {"content": "..."}}
          - dict：完整透传（可含 content / custom_template_id / params）
          - 模板：{"custom_template_id": "...", "params": [...]}

        quote_msg_idx: 引用回复（官方 message_reference）。取值必须是**被引用消息的
          msg_idx**（形如 REFIDX_xxxxxx==），而不是 msg_id —— 官方文档明确
          message_reference.message_id 接收的是索引，两者不可混用：
            - 别人发的消息：从消息事件 message_scene.ext 的 msg_idx 字段取
              （本项目已在 parse_message 中解析并随历史记录保存）
            - 机器人自己发的消息：从发消息响应 ext_info.ref_idx 取
              （本项目已保存为该条历史的 msg_idx，见 get_last_send_msg_idx）
          因此这里只接受 msg_idx；消息类型不限（文本/媒体/引用可同发）。

        注意：富媒体(msg_type=7)与 Markdown(msg_type=2)互斥，同时传入时富媒体优先。
        返回: 消息ID（str）表示成功，None 表示失败。
        """
        # 日志标识：app_id + 会话（msg_type=group_xxx / c2c_xxx）+ 触发本次发送的 msg_id
        ctx = LogCtx(app_id=self.app_id,
                     thread_key=f"{'c2c' if msg_type == 'c2c' else 'group'}_{recipient_id}",
                     msg_id=msg_id or "")
        # ---------- 1. 获取 file_info ----------
        file_info = None
        if media_source:
            if not (media_source.startswith('http://') or media_source.startswith('https://')):
                warn(f"[发送] 错误：media_source 必须为 HTTP/HTTPS URL，收到: {media_source[:80]}", ctx=ctx)
                return None

            info(f"[发送] 调用上传，file_name={file_name}, file_type={file_type}", ctx=ctx)
            file_info = await self.upload_media_by_url(msg_type, recipient_id,
                                                       media_source, file_type, file_name)
            if not file_info:
                error("[发送] URL 上传失败，放弃发送", ctx=ctx)
                return None

        if not media_source and media:
            file_info = media

        # ---------- 2. 确定 msg_seq ----------
        if msg_id:
            if msg_seq is None:
                # 自动分配序列号
                self._msg_seq_map[msg_id] = self._msg_seq_map.get(msg_id, 0) + 1
                msg_seq = self._msg_seq_map[msg_id]
        else:
            msg_seq = None  # 主动消息可不填

        # ---------- 3. 发送消息（重试） ----------
        is_media_msg = (file_info is not None)
        media_obj = {"file_info": file_info} if is_media_msg else None

        for attempt in range(max_retries):
            token = await self.get_access_token_async()
            if msg_type == "c2c":
                url = f"https://api.sgroup.qq.com/v2/users/{recipient_id}/messages"
            else:
                url = f"https://api.sgroup.qq.com/v2/groups/{recipient_id}/messages"
            headers = {
                "Authorization": f"QQBot {token}",
                "Content-Type": "application/json"
            }
            payload: Dict[str, Any] = {}

            # ---------- 归一化 markdown 参数 ----------
            md_obj = None
            if markdown is not None and not is_media_msg:
                if isinstance(markdown, str):
                    md_obj = {"content": markdown}
                elif isinstance(markdown, dict):
                    md_obj = dict(markdown)
                else:
                    warn(f"[发送] markdown 参数类型不支持: {type(markdown).__name__}，已忽略", ctx=ctx)
                if md_obj is not None and not md_obj:
                    md_obj = None

            if md_obj is not None:
                # Markdown 消息：msg_type=2，内容放在 markdown 对象里（不再用 content）
                payload["markdown"] = md_obj
            elif content:
                payload["content"] = content

            if msg_id:
                payload["msg_id"] = msg_id
            if msg_seq is not None:
                payload["msg_seq"] = msg_seq
            if embed:
                payload["embed"] = embed
            # ---------- 引用回复（message_reference）----------
            # 官方两种响应体（单聊/群聊一致）：{"message_id": "<被引用消息的 msg_idx>"}。
            # 注意字段名是 message_id 但收的是**索引**（REFIDX_...），
            # 传真正的消息 id 会被判为无效（40034024 / 304061），
            # 所以参数入口就叫 quote_msg_idx，避免调用方误传 msg_id。
            # 与 msg_id 无关：引用是"展示形式"，被动回复是"回复凭证"，两者可同时存在。
            # 先 strip 再判空：空白串不是有效索引，落到 payload 里会变成
            # {"message_id": ""} 被服务端判为无效引用（40034024）。
            _quote_idx = str(quote_msg_idx).strip() if quote_msg_idx else ""
            if _quote_idx:
                payload["message_reference"] = {"message_id": _quote_idx}
            if is_media_msg:
                payload["msg_type"] = 7
                payload["media"] = media_obj
            elif md_obj is not None:
                payload["msg_type"] = 2
            else:
                payload["msg_type"] = 0

            debug(f"[发送] 最终 payload: {json.dumps(payload, ensure_ascii=False)[:500]}", ctx=ctx)
            loop = asyncio.get_event_loop()
            try:
                resp = await loop.run_in_executor(
                    get_executor(),
                    lambda: requests.post(url, json=payload, headers=headers, timeout=10)
                )
                if resp.status_code == 200:
                    msg_id_resp = self._parse_send_response(resp)
                    if is_media_msg:
                        info(f"[Reply] {msg_type} 富媒体消息发送成功: {content[:50] if content else '媒体'} msg_id={msg_id_resp}", ctx=ctx)
                    elif md_obj is not None:
                        preview = str(md_obj.get("content", ""))[:50]
                        kind = "模板" if md_obj.get("custom_template_id") else "自定义"
                        info(f"[Reply] {msg_type} Markdown({kind})消息发送成功: {preview} msg_id={msg_id_resp}", ctx=ctx)
                    else:
                        info(f"[Reply] {msg_type} 发送成功: {content[:50] if content else '空'} msg_id={msg_id_resp}", ctx=ctx)
                    return msg_id_resp
                else:
                    error(f"[Reply Error] 尝试 {attempt+1}/{max_retries} 失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}", ctx=ctx)
                    if resp.status_code in (401, 500):
                        warn("[Reply] 强制刷新 Token", ctx=ctx)
                        # 必须用 async 包装：这是跨公网请求，同步调用会
                        # 在重试路径上卡住整个事件循环（所有机器人共用）
                        await self.get_access_token_async(force_refresh=True)
                        continue
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        error("[Reply] 发送最终失败，放弃消息", ctx=ctx)
                        # ----- 被动回复超限 → 自动切主动重试 -----
                        if msg_id:
                            try:
                                resp_json = resp.json()
                                err_code = resp_json.get("code") or resp_json.get("err_code")
                            except Exception:
                                err_code = None
                            if err_code == 40034128:
                                warn("[Reply] 检测到被动回复超限（40034128），尝试主动发送...", ctx=ctx)
                                # 去掉 msg_id 以主动发送
                                payload.pop("msg_id", None)
                                payload.pop("msg_seq", None)
                                active_resp = await loop.run_in_executor(
                                    get_executor(),
                                    lambda: requests.post(url, json=payload, headers=headers, timeout=10)
                                )
                                if active_resp.status_code == 200:
                                    msg_id_resp = self._parse_send_response(active_resp)
                                    info(f"[Reply] 主动发送成功: {content[:50] if content else '空'} msg_id={msg_id_resp}", ctx=ctx)
                                    return msg_id_resp
                                else:
                                    error(f"[Reply] 主动发送也失败，状态码 {active_resp.status_code}", ctx=ctx)
                        return None
            except Exception as e:
                error(f"[Reply Error] 尝试 {attempt+1}/{max_retries} 异常: {e}", ctx=ctx)
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    error("[Reply] 发送最终失败，放弃消息", ctx=ctx)
                    return None
        return None

    # ========== 撤回消息 ==========
    async def revoke_message(self, msg_type: str, recipient_id: str, message_id: str) -> bool:
        """
        撤回一条已发送的消息。
        msg_type: "c2c" 或 "group"
        recipient_id: 用户 openid 或群 openid
        message_id: 要撤回的消息 ID
        返回 True 表示撤回成功，False 表示失败。
        注意：发送超过2分钟的消息不可撤回。
        """
        # 日志标识：app_id + 会话（msg_type=group_xxx / c2c_xxx）
        ctx = LogCtx(app_id=self.app_id,
                     thread_key=f"{'c2c' if msg_type == 'c2c' else 'group'}_{recipient_id}")
        token = await self.get_access_token_async()
        if msg_type == "c2c":
            url = f"https://api.sgroup.qq.com/v2/users/{recipient_id}/messages/{message_id}"
        else:
            url = f"https://api.sgroup.qq.com/v2/groups/{recipient_id}/messages/{message_id}"
        headers = {
            "Authorization": f"QQBot {token}",
        }
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.delete(url, headers=headers, timeout=10)
            )
            if resp.status_code == 200:
                info(f"[撤回] {msg_type} 消息撤回成功: {message_id} (target={recipient_id})", ctx=ctx)
                return True
            else:
                error(f"[撤回] 失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}", ctx=ctx)
                return False
        except Exception as e:
            error(f"[撤回] 异常: {e}", ctx=ctx)
            return False

    # ========== 群禁言相关 API ==========
    async def get_group_mute_status(self, group_openid: str) -> Optional[Dict]:
        ctx = LogCtx(app_id=self.app_id, thread_key=f"group_{group_openid}")
        token = await self.get_access_token_async()
        url = f"https://api.sgroup.qq.com/v2/groups/{group_openid}/restrict_chat_setting"
        headers = {"Authorization": f"QQBot {token}"}
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.get(url, headers=headers, timeout=10)
            )
            if resp.status_code == 200:
                return resp.json()
            else:
                warn(f"[群禁言] 查询失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}", ctx=ctx)
                return None
        except Exception as e:
            warn(f"[群禁言] 查询异常: {e}", ctx=ctx)
            return None

    async def get_group_info(self, group_openid: str) -> Optional[Dict]:
        """
        获取群基本信息。
        GET /v2/groups/{group_openid}/info
        返回: {"group_openid": "...", "group_name": "...", ...} 或 None
        """
        ctx = LogCtx(app_id=self.app_id, thread_key=f"group_{group_openid}")
        token = await self.get_access_token_async()
        url = f"https://api.sgroup.qq.com/v2/groups/{group_openid}/info"
        headers = {"Authorization": f"QQBot {token}"}
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.get(url, headers=headers, timeout=10)
            )
            if resp.status_code == 200:
                data = resp.json()
                info(f"[群信息] 获取成功: {data.get('group_name', '未知')} ({group_openid})", ctx=ctx)
                return data
            else:
                error(f"[群信息] 获取失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}", ctx=ctx)
                return None
        except Exception as e:
            error(f"[群信息] 获取异常: {e}", ctx=ctx)
            return None

    async def get_bot_state(self, group_openid: str) -> Optional[Dict]:
        """
        获取机器人在指定群中的状态信息。
        GET /v2/groups/{group_openid}/bot_state
        返回: 包含 member_role, allow_proactive_msg, recv_msg_setting 等
        """
        ctx = LogCtx(app_id=self.app_id, thread_key=f"group_{group_openid}")
        token = await self.get_access_token_async()
        url = f"https://api.sgroup.qq.com/v2/groups/{group_openid}/bot_state"
        headers = {"Authorization": f"QQBot {token}"}
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.get(url, headers=headers, timeout=10)
            )
            if resp.status_code == 200:
                data = resp.json()
                info(f"[机器人状态] 获取成功: role={data.get('member_role')}, proactive={data.get('allow_proactive_msg')} ({group_openid})", ctx=ctx)
                return data
            else:
                error(f"[机器人状态] 获取失败，状态码 {resp.status_code}, 响应: {resp.text[:200]}", ctx=ctx)
                return None
        except Exception as e:
            error(f"[机器人状态] 获取异常: {e}", ctx=ctx)
            return None

    async def set_group_mute(self, group_openid: str, op: str, member_openid: str,
                             mute_expire_at: str = "") -> Tuple[bool, int]:
        ctx = LogCtx(app_id=self.app_id, thread_key=f"group_{group_openid}")
        token = await self.get_access_token_async()
        url = f"https://api.sgroup.qq.com/v2/groups/{group_openid}/restrict_chat_setting"
        headers = {
            "Authorization": f"QQBot {token}",
            "Content-Type": "application/json"
        }
        payload = {
            "members": [
                {
                    "op": op,
                    "member_openid": member_openid,
                    "mute_expire_at": mute_expire_at
                }
            ]
        }
        debug(f"[群禁言] 发送请求: POST {url}", ctx=ctx)
        debug(f"[群禁言] payload: {json.dumps(payload, ensure_ascii=False)}", ctx=ctx)
        loop = asyncio.get_event_loop()
        try:
            resp = await loop.run_in_executor(
                get_executor(),
                lambda: requests.post(url, json=payload, headers=headers, timeout=10)
            )
            if resp.status_code == 200:
                return True, 0
            else:
                error_code = 0
                try:
                    err_data = resp.json()
                    error_code = err_data.get("err_code", err_data.get("code", 0))
                except Exception:
                    pass
                error(f"[群禁言] 设置失败，状态码 {resp.status_code}, 错误码 {error_code}, 响应: {resp.text[:500]}", ctx=ctx)
                return False, error_code
        except Exception as e:
            error(f"[群禁言] 设置异常: {e}", ctx=ctx)
            return False, -1


# ==================== WebSocket 主连接 ====================
async def main_connection(bot_client: BotClient):
    from msg import handle_message, handle_event

    # 日志标识：整个连接生命周期内的日志都带本机器人的 app_id
    ctx = LogCtx(app_id=bot_client.app_id)

    info(f"[启动] 机器人 {bot_client.app_id} 开始连接...", ctx=ctx)
    # 用 async 包装：内部是同步 requests 的跨公网请求，直接调用会卡住
    # 整个事件循环（所有机器人在同一个循环上）
    ws_url = await bot_client.get_websocket_url_async()
    info(f"[启动] 地址: {ws_url}", ctx=ctx)

    async with websockets.connect(ws_url) as ws:
        hello = await ws.recv()
        hello_data = json.loads(hello)
        debug(f"[收到 Hello] {hello}", ctx=ctx)
        heartbeat_interval = hello_data.get("d", {}).get("heartbeat_interval", 30000) / 1000.0

        token = await bot_client.get_access_token_async()
        identify = {
            "op": 2,
            "d": {
                "token": f"QQBot {token}",
                "intents": (1 << 25) | (1 << 30) | (1 << 24),
                "shard": [0, 1],
                "properties": {"os": "Linux", "browser": "MyBot", "device": "MyBot"}
            }
        }
        await ws.send(json.dumps(identify))
        debug("[鉴权] 已发送 Identify", ctx=ctx)

        while True:
            msg = await ws.recv()
            data = json.loads(msg)
            if data.get("op") == 0 and data.get("t") == "READY":
                ready_data = data.get("d", {})
                user_info = ready_data.get("user", {})
                bot_username = user_info.get("username", "蓝狼")
                bot_client.bot_name = bot_username
                set_bot_name(bot_username)
                info(f"[收到 Ready] 机器人名字: {bot_username}", ctx=ctx)
                break
            else:
                debug(f"[收到] {msg}", ctx=ctx)

        info(f"[准备就绪] 机器人 {bot_client.bot_name} 已上线", ctx=ctx)

        async def heartbeat():
            while True:
                await asyncio.sleep(heartbeat_interval)
                try:
                    await ws.send(json.dumps({"op": 1, "d": int(time.time() * 1000)}))
                    debug("[心跳] 发送", ctx=ctx)
                except websockets.ConnectionClosed:
                    info("[心跳] 连接已关闭", ctx=ctx)
                    break
                except Exception as e:
                    warn(f"[心跳错误] {e}", ctx=ctx)
                    break

                # ★ 使用时主动检测：bot 是否仍存在于配置且启用
                still_valid = False
                for b in get_bots():
                    if b.get("APP_ID") == bot_client.app_id and get_bot_enabled(bot_client.app_id):
                        still_valid = True
                        break
                if not still_valid:
                    info(f"[心跳] 机器人 {bot_client.app_id} 已被移除或禁用，关闭连接", ctx=ctx)
                    await ws.close()
                    break

        _spawn_background(heartbeat(), f"heartbeat-{bot_client.app_id}", ctx=ctx)
        info("[监听] 开始接收消息...", ctx=ctx)

        async for raw in ws:
            try:
                data = json.loads(raw)
                op = data.get("op")
                if op not in (1, 11):
                    debug(f"[收到] {raw[:200]}...", ctx=ctx)
                if op == 0:
                    t = data.get("t")
                    if t in ("C2C_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"):
                        if get_media_block() == 1:
                            # 阻塞模式（旧逻辑）：串行等待，识别期间后续消息读不进来。
                            # 实时读取配置，改完即生效。
                            await handle_message(data, bot_client)
                        else:
                            # 非阻塞模式（默认）：handle_message 内部可能包含媒体识别
                            # （耗时数秒），若在此串行等待，后续消息就读不进来，媒体识别
                            # 也就永远等不到"新消息"来打断它。改为派发成任务，让接收循环
                            # 持续收消息。
                            #
                            # 并发安全性由 handle_message 自身保证：
                            #   - 每条消息先 bump_media_gen（打断同会话正在进行的识别）
                            #   - 会话级的队列/任务调度（pending_queues / pending_process_tasks）
                            #     负责同一会话的串行与合并，不同会话互不影响。
                            task = asyncio.create_task(handle_message(data, bot_client))
                            _message_tasks.add(task)
                            # 结束时移除引用，并读一次异常：
                            # 只 discard 不取异常的话，handle_message 里的报错
                            # 只会以 "Task exception was never retrieved" 落到
                            # stderr，不进日志，事后无从排查。
                            task.add_done_callback(_log_message_task_result)
                    elif t in ("GROUP_JOIN_REQUEST", "GROUP_MEMBER_ADD", "GROUP_MEMBER_REMOVE"):
                        await handle_event(data, bot_client)
                    else:
                        pass
            except websockets.ConnectionClosed:
                info("[连接] 服务器关闭连接", ctx=ctx)
                raise
            except Exception as e:
                error(f"[处理消息错误] {e}", ctx=ctx)


# ==================== 媒体发送编排（单一事实来源） ====================
async def send_with_policy(sender: BotClient, msg_type: str, recipient_id: str,
                           content: str, *, media_source: str = None,
                           file_type: int = None, file_name: str = None,
                           markdown: str = None, footer: str = "",
                           msg_id: str = None, interval: float = 0.3) -> Dict[str, Any]:
    """
    按媒体类型策略发送消息，统一 push_message / schedule_push 等多处推送逻辑。

    策略（由 utils.needs_dual_send 决定）：
      - 图片(1)：content 文字随媒体一起显示 → 一条媒体消息即可
      - 视频/语音/文件(2/3/4)：content 会被 QQ 客户端忽略 → 先发媒体，再单独发文本
        仅当确实有文字（caption/markdown/footer 任一非空）时才双发，否则单发。

    失败语义：
      - 媒体发送失败 → 立即中止，**不再发送文本**，整条消息按失败返回
        （推送场景媒体是主体，只留一条说明文字会造成语义残缺）；
      - 媒体成功、文本失败 → 仍算成功（媒体已送达）。

    参数：
      content:   媒体消息的正文（不带 footer）
      markdown:  非空且 use_md 时走 msg_type=2 富文本；媒体消息本身不能用 Markdown
      footer:    追加到文本消息末尾的说明（如"来自 XX 发起的推送"）
      msg_id:    传入则作为被动回复（QQ 要求被动回复带 msg_id/msg_seq）。
                 双发场景下仅第一条带 msg_id，第二条为主动消息。
      interval:  双发之间的间隔秒数

    返回：{"ok": bool, "error": str, "message_ids": [{"msg_id","msg_idx","type"}]}
      message_ids 按实际发送顺序排列，双发场景含 2 条，调用方据此向模型暴露全部消息ID。
    """
    # 日志标识：app_id 来自 sender，thread_key 由本次发送目标构造
    ctx = LogCtx(app_id=sender.app_id,
                 thread_key=f"{'c2c' if msg_type == 'c2c' else 'group'}_{recipient_id}",
                 msg_id=msg_id or "")
    result = {"ok": False, "error": "", "message_ids": []}

    def _full(base: str) -> str:
        return (base + footer) if footer else base

    try:
        # 双发判定：类型需要双发，且确实有文字要展示（caption/markdown/footer 任一非空）。
        # 无文字时单发，避免产生一条空的文本消息。
        _has_text = bool((content or "").strip() or (markdown or "").strip()
                         or (footer or "").strip())
        if media_source and needs_dual_send(file_type, file_name, media_source) and _has_text:
            # ---- 双发：先媒体，后独立文本 ----
            # 媒体消息本身只带正文（不带 footer），避免装饰文字被媒体消息吞掉；
            # footer 统一由随后的独立文本消息承载。
            sender.begin_send_batch()
            msg_id_media = await sender.send_message(
                msg_type=msg_type, recipient_id=recipient_id,
                content=content,
                media_source=media_source, file_type=file_type, file_name=file_name,
                msg_id=msg_id,
            )
            media_idx = sender.get_call_sent()[0]["msg_idx"] if sender.get_call_sent() else None

            # 媒体发送失败 → 立刻中止，不再发送文本。
            # 推送场景下媒体是主体，只留下一条"说明文字"会造成语义残缺，
            # 因此整条消息按失败处理，由调用方计入 fail_count。
            if not msg_id_media:
                result["ok"] = False
                result["error"] = "媒体发送失败，已中止发送文本"
                return result

            result["message_ids"].append(
                {"msg_id": msg_id_media, "msg_idx": media_idx, "type": "media"})

            await asyncio.sleep(interval)

            text_kwargs = {"msg_type": msg_type, "recipient_id": recipient_id, "msg_id": None}
            if markdown:
                text_kwargs["markdown"] = _full(markdown)
            else:
                text_kwargs["content"] = _full(content)
            sender.begin_send_batch()
            msg_id_text = await sender.send_message(**text_kwargs)
            text_idx = sender.get_call_sent()[0]["msg_idx"] if sender.get_call_sent() else None

            if msg_id_text:
                result["message_ids"].append({
                    "msg_id": msg_id_text,
                    "msg_idx": text_idx,
                    "type": "markdown" if markdown else "text",
                })
            # 媒体已成功，文本失败不影响整体成功（媒体本身已送达）
            result["ok"] = True

        elif media_source:
            # ---- 图片：一条媒体消息带正文（图片的正文会正常显示，footer 也随之一并带上）----
            sender.begin_send_batch()
            msg_id_sent = await sender.send_message(
                msg_type=msg_type, recipient_id=recipient_id,
                content=_full(content),
                media_source=media_source, file_type=file_type, file_name=file_name,
                msg_id=msg_id,
            )
            sent_idx = sender.get_call_sent()[0]["msg_idx"] if sender.get_call_sent() else None
            if msg_id_sent:
                result["message_ids"].append(
                    {"msg_id": msg_id_sent, "msg_idx": sent_idx, "type": "media_combined"})
            result["ok"] = bool(msg_id_sent)
            if not result["ok"]:
                result["error"] = "发送失败"

        else:
            # ---- 纯文本 / Markdown ----
            kwargs = {"msg_type": msg_type, "recipient_id": recipient_id, "msg_id": None}
            if markdown:
                kwargs["markdown"] = _full(markdown)
            else:
                kwargs["content"] = _full(content)
            sender.begin_send_batch()
            msg_id_sent = await sender.send_message(**kwargs)
            sent_idx = sender.get_call_sent()[0]["msg_idx"] if sender.get_call_sent() else None
            if msg_id_sent:
                result["message_ids"].append({
                    "msg_id": msg_id_sent,
                    "msg_idx": sent_idx,
                    "type": "markdown" if markdown else "text",
                })
            result["ok"] = bool(msg_id_sent)
            if not result["ok"]:
                result["error"] = "发送失败"

    except Exception as e:
        error(f"[发送编排] 异常: {e}", ctx=ctx)
        result["error"] = f"发送异常: {e}"

    return result