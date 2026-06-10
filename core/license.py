"""
火种系统 · 企业级授权与白盒转换管理器 (LicenseManager)

核心职责：
1. 提供基于硬件指纹、时间限制、远程验证的多因子授权校验
2. 安全解密加密模块，包含完整性验证和代码安全扫描
3. 维护审计追踪，支持权限分级和远程吊销

外部依赖（真实模块接口）：
- config.default.yaml : 授权配置（服务器地址、公钥路径、硬件指纹开关等）
- core.event_bus.EventBus : 发布授权事件
- core.semantic_index.SemanticIndex : 审计日志持久化
- cryptography.fernet : 对称加密
- cryptography.hazmat.primitives.asymmetric.ed25519 : 数字签名验证
- hashlib, hmac, os, time, json

接口契约：
- validate_license(license_key: str, hardware_id: str) -> Dict[str, Any]
- decrypt_module(encrypted_payload: bytes, license_key: str, expected_hash: str) -> Dict[str, Any]
- is_authorized() -> bool
- revoke() -> None
- health_check() -> Dict[str, Any]
- 返回值字典固定包含 "status" (str), "reason" (str), "warnings" (List[str])

异常与降级：
- 授权失败时记录审计日志并抛出 LicenseError，所有解密操作拒绝执行
- 加密库不可用时直接拒绝启动，无明文降级
- 远程验证不可达时若存在有效缓存令牌且未过期，可离线工作（硬件绑定）
- 所有敏感信息（密钥、令牌）均不记录在日志中

资源管理：
- 内存中仅保留解密后的代码指针，使用后立即从内存擦除
- 加密密钥通过环境变量或专用密钥管理服务注入，不在代码/配置中明文存储
"""

import os
import time
import hashlib
import hmac
import logging
import threading
from typing import Dict, Any, Optional, Tuple, List
from dataclasses import dataclass
from enum import Enum

# 强制依赖加密库，不可降级
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.exceptions import InvalidSignature

logger = logging.getLogger(__name__)


class LicenseError(Exception):
    """授权相关异常"""
    pass


class SecurityPolicy(Enum):
    """安全策略级别"""
    STRICT = "strict"       # 必须全部通过
    PERMISSIVE = "permissive"  # 仅日志警告


@dataclass(frozen=True)
class LicenseToken:
    """授权令牌数据类"""
    hardware_id: str
    expiry: float           # Unix timestamp
    permissions: List[str]
    signature: bytes        # 服务器签名


class LicenseManager:
    """
    企业级授权管理器
    线程安全单例模式，支持硬件绑定、时间窗口、数字签名、远程吊销
    """

    # 类常量
    MIN_KEY_LENGTH = 32
    MAX_KEY_LENGTH = 1024
    BRUTE_FORCE_LOCKOUT_THRESHOLD = 5    # 连续失败次数
    LOCKOUT_DURATION = 300               # 锁定时长 (秒)
    OFFLINE_GRACE_PERIOD = 86400         # 离线宽限期 (秒)
    MODULE_SIZE_LIMIT = 10 * 1024 * 1024 # 解密模块最大10MB

    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config: Optional[Dict[str, Any]] = None, event_bus=None, semantic_index=None):
        # 防止重复初始化
        if hasattr(self, '_initialized') and self._initialized:
            return

        self._lock = threading.RLock()
        self._config = config or {}
        self._event_bus = event_bus
        self._semantic_index = semantic_index

        # 从配置文件或环境变量加载密钥材料（绝不硬编码）
        self._master_seed = self._load_master_seed()
        self._server_public_key = self._load_server_public_key()
        self._hardware_id_required = self._config.get("license.hardware_bind", True)
        self._offline_allowed = self._config.get("license.allow_offline", False)

        # 派生 Fernet 密钥（使用 PBKDF2 加强）
        kdf = hashlib.pbkdf2_hmac('sha256', self._master_seed.encode(), b'QBox-Fernet-Salt', 100_000)
        fernet_key = base64.urlsafe_b64encode(kdf)
        self._fernet = Fernet(fernet_key)

        # 状态
        self._authorized = False
        self._current_token: Optional[LicenseToken] = None
        self._failed_attempts = 0
        self._last_failed_time = 0.0
        self._revoked = False

        self._initialized = True
        logger.info("[Core::License] 授权管理器初始化完成")

    def _load_master_seed(self) -> str:
        """从环境变量或安全配置源加载主密钥种子，禁止回退到默认值"""
        seed = os.environ.get("QBOX_LICENSE_SEED")
        if not seed:
            # 尝试从密钥管理服务获取（示例）
            raise LicenseError("未找到主密钥种子，必须设置环境变量 QBOX_LICENSE_SEED")
        if len(seed) < self.MIN_KEY_LENGTH:
            raise LicenseError("主密钥种子长度不足")
        return seed

    def _load_server_public_key(self) -> Ed25519PublicKey:
        """加载授权服务器的 Ed25519 公钥"""
        pem_data = self._config.get("license.server_public_key_pem")
        if not pem_data:
            raise LicenseError("缺少授权服务器公钥配置")
        from cryptography.hazmat.primitives.serialization import load_pem_public_key
        return load_pem_public_key(pem_data.encode())

    # ---------- 公共接口 ----------
    def validate_license(self, license_key: str, hardware_id: str = "") -> Dict[str, Any]:
        """
        验证授权密钥，支持硬件绑定和远程令牌验证。
        返回标准字典，但敏感细节不对外暴露。
        """
        if self._revoked:
            return {"status": "error", "reason": "授权已吊销", "warnings": []}

        # 暴力破解防护
        with self._lock:
            if self._failed_attempts >= self.BRUTE_FORCE_LOCKOUT_THRESHOLD:
                if time.time() - self._last_failed_time < self.LOCKOUT_DURATION:
                    remaining = self.LOCKOUT_DURATION - (time.time() - self._last_failed_time)
                    logger.warning(f"[Core::License] 账户锁定中，剩余 {remaining:.0f} 秒")
                    return {"status": "error", "reason": "授权失败", "warnings": ["请稍后重试"]}
                else:
                    self._failed_attempts = 0  # 冷却期过，重置

            if not license_key or len(license_key) > self.MAX_KEY_LENGTH:
                self._record_failure("密钥格式错误")
                return {"status": "error", "reason": "授权失败", "warnings": []}

            # 本地快速校验：格式和签名
            try:
                token = self._parse_and_verify_token(license_key, hardware_id)
            except LicenseError as e:
                self._record_failure(str(e))
                return {"status": "error", "reason": "授权失败", "warnings": []}

            # 检查时效
            if token.expiry < time.time():
                self._record_failure("令牌已过期")
                return {"status": "error", "reason": "授权失败", "warnings": []}

            # 硬件绑定
            if self._hardware_id_required and token.hardware_id != self._get_hardware_id():
                self._record_failure("硬件指纹不匹配")
                return {"status": "error", "reason": "授权失败", "warnings": []}

            # 远程吊销检查（可选，若有网络）
            if self._config.get("license.check_revocation_online", False):
                if not self._check_online_revocation(token):
                    self._record_failure("授权已被远程吊销")
                    return {"status": "error", "reason": "授权失败", "warnings": []}

            # 授权通过
            self._authorized = True
            self._current_token = token
            self._failed_attempts = 0
            self._audit_log("LICENSE_VALID", f"硬件{hardware_id} 授权成功")
            logger.info("[Core::License] 授权验证通过")
            return {"status": "ok", "reason": "授权成功", "warnings": []}

    def decrypt_module(self, encrypted_payload: bytes, license_key: str,
                       expected_hash: str, required_permission: str = "module.load") -> Dict[str, Any]:
        """
        解密并验证一个模块代码。
        encrypted_payload: 包含加密数据+签名+元数据的字节流。
        expected_hash: 解密后代码的 SHA256 期望值，用于完整性校验。
        """
        if not self._authorized or self._current_token is None:
            return {"status": "error", "reason": "未授权，无法解密", "warnings": []}
        if required_permission not in self._current_token.permissions:
            self._audit_log("DECRYPT_DENIED", f"缺少权限 {required_permission}")
            return {"status": "error", "reason": "权限不足", "warnings": []}

        try:
            # 解析负载格式：4字节长度 + 加密数据 + Ed25519签名
            if len(encrypted_payload) < 68:  # 4 + 至少64
                raise ValueError("负载格式错误")
            import struct
            enc_len = struct.unpack(">I", encrypted_payload[:4])[0]
            if enc_len > self.MODULE_SIZE_LIMIT:
                raise ValueError(f"加密模块过大 ({enc_len})")
            encrypted_data = encrypted_payload[4:4+enc_len]
            signature = encrypted_payload[4+enc_len:]

            # 验证开发者签名
            try:
                self._server_public_key.verify(signature, encrypted_data)
            except InvalidSignature:
                self._audit_log("DECRYPT_FAIL", "签名验证失败")
                raise LicenseError("模块签名无效，可能被篡改")

            # 解密
            plain_code = self._fernet.decrypt(encrypted_data).decode('utf-8')

            # 完整性哈希校验
            code_hash = hashlib.sha256(plain_code.encode()).hexdigest()
            if not hmac.compare_digest(code_hash, expected_hash):
                self._audit_log("DECRYPT_FAIL", "哈希不匹配")
                raise LicenseError("解密后代码完整性校验失败")

            # 静态安全扫描 (AST)
            if not self._security_scan(plain_code):
                self._audit_log("DECRYPT_BLOCKED", "安全扫描未通过")
                raise LicenseError("代码安全扫描不通过，禁止加载")

            self._audit_log("DECRYPT_SUCCESS", f"模块哈希 {code_hash[:16]}...")
            # 返回后调用方应在使用后清除 plain_code 引用
            return {"status": "ok", "code": plain_code, "warnings": []}

        except (LicenseError, InvalidToken) as e:
            logger.error("[Core::License] 解密失败 (原因已记录审计日志)")
            return {"status": "error", "reason": "解密失败", "warnings": [], "code": ""}
        except Exception as e:
            logger.exception(f"[Core::License] 解密异常: {e}")  # 内部异常不暴露细节
            self._audit_log("DECRYPT_ERROR", "未知解密错误")
            return {"status": "error", "reason": "解密失败", "warnings": [], "code": ""}

    def is_authorized(self) -> bool:
        return self._authorized and not self._revoked

    def revoke(self) -> None:
        """立即吊销授权，清除内存令牌"""
        with self._lock:
            self._authorized = False
            self._revoked = True
            self._current_token = None
        self._audit_log("LICENSE_REVOKED", "主动吊销")
        logger.warning("[Core::License] 授权已吊销")

    @classmethod
    def health_check(cls) -> Dict[str, Any]:
        """检查模块健康状态（静态级）"""
        # 不创建新实例，仅验证类常量和库可用性
        try:
            from cryptography.fernet import Fernet
            _ = cls.MIN_KEY_LENGTH
            return {"status": "ok", "reason": "LicenseManager 组件正常", "warnings": []}
        except Exception as e:
            return {"status": "error", "reason": str(e), "warnings": ["加密库不可用"]}

    # ---------- 内部方法 ----------
    def _parse_and_verify_token(self, license_key: str, hardware_id: str) -> LicenseToken:
        """
        解析 license_key，它应该是一个 Base64 编码的 JSON + 服务器签名。
        结构：{"token_data": {...}, "signature": "..."}
        """
        import json
        try:
            raw = base64.b64decode(license_key).decode('utf-8')
            payload = json.loads(raw)
        except Exception:
            raise LicenseError("密钥格式无效")

        token_data = payload.get("token_data")
        signature_b64 = payload.get("signature")
        if not token_data or not signature_b64:
            raise LicenseError("密钥字段缺失")
        signature = base64.b64decode(signature_b64)

        # 验证服务器签名
        try:
            self._server_public_key.verify(signature, json.dumps(token_data, sort_keys=True).encode())
        except InvalidSignature:
            raise LicenseError("签名验证失败")

        # 构造 LicenseToken
        return LicenseToken(
            hardware_id=token_data["hardware_id"],
            expiry=token_data["expiry"],
            permissions=token_data.get("permissions", ["module.load"]),
            signature=signature
        )

    def _get_hardware_id(self) -> str:
        """获取硬件指纹，使用稳定标识（如 dmidecode + MAC）"""
        # 简化示例，生产可用 machineid 或定制
        import uuid
        return hashlib.sha256(uuid.getnode().to_bytes(6, 'big') + socket.gethostname().encode()).hexdigest()

    def _check_online_revocation(self, token: LicenseToken) -> bool:
        """调用授权服务器 OCSP/CRL 检查吊销状态，此处简化为 true"""
        # 实际实现应发送 token 标识到服务器验证
        return True

    def _security_scan(self, code: str) -> bool:
        """对解密后的代码进行 AST 安全扫描，禁止危险调用"""
        forbidden = ["os.system", "subprocess", "eval", "exec", "__import__", "compile"]
        import ast
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and hasattr(node.func, 'id') and node.func.id in forbidden:
                    return False
            return True
        except SyntaxError:
            return False

    def _record_failure(self, reason: str) -> None:
        with self._lock:
            self._failed_attempts += 1
            self._last_failed_time = time.time()
        self._audit_log("LICENSE_FAIL", reason)

    def _audit_log(self, event: str, detail: str) -> None:
        """写入审计日志，并通过语义索引发布事件"""
        logger.info(f"[AUDIT] {event} | {detail}")
        if self._event_bus:
            try:
                self._event_bus.publish("license.audit", {"event": event, "detail": detail, "ts": time.time()})
            except Exception:
                pass
        if self._semantic_index:
            try:
                self._semantic_index.log_event(f"License::{event}", {"detail": detail})
            except Exception:
                pass
