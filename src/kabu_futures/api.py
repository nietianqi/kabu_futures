from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import ApiConfig


class KabuApiError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        details: str | None = None,
        category: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.details = details
        self.category = category or classify_kabu_api_error(message)


WRONG_INSTANCE_MESSAGE = "別のPCでkabuステーションが起動"


def classify_kabu_api_error(error: object) -> str:
    if isinstance(error, KabuApiError) and error.category:
        return error.category
    text = str(error)
    if "WinError 10054" in text or "10054" in text:
        return "websocket_remote_closed"
    if WRONG_INSTANCE_MESSAGE in text:
        return "kabu_station_wrong_instance"
    if "auth_recovery_failed" in text:
        return "auth_recovery_failed"
    if "HTTP 401" in text or '"Code":4001007' in text or "ログイン認証エラー" in text:
        return "auth_error"
    if "HTTP 400" in text:
        return "bad_request"
    if "HTTP 403" in text:
        return "forbidden"
    if "HTTP 429" in text or '"Code":4001006' in text:
        return "rate_limit"
    if "HTTP 503" in text:
        return "service_unavailable"
    if "HTTP 500" in text:
        return "server_error"
    return "kabu_api"


class KabuStationClient:
    def __init__(self, password: str, config: ApiConfig | None = None, production: bool = False) -> None:
        self.config = config or ApiConfig()
        self.password = password
        self.base_url = self.config.production_url if production else self.config.sandbox_url
        self.token: str | None = None

    def authenticate(self) -> str:
        response = self._request("POST", "/token", {"APIPassword": self.password}, auth=False)
        token = response.get("Token")
        if not isinstance(token, str) or not token:
            raise KabuApiError("Token was not returned by kabu Station")
        self.token = token
        return token

    def symbolname_future(self, future_code: str, deriv_month: int = 0) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/symbolname/future?{urlencode({'FutureCode': future_code, 'DerivMonth': deriv_month})}",
            None,
            retry_auth=True,
        )

    def register(self, symbols: list[dict[str, Any]]) -> dict[str, Any]:
        return self._request("PUT", "/register", {"Symbols": symbols})

    def unregister(self, symbols: list[dict[str, Any]]) -> dict[str, Any]:
        return self._request("PUT", "/unregister", {"Symbols": symbols})

    def unregister_all(self) -> dict[str, Any]:
        return self._request("PUT", "/unregister/all", None)

    def board(self, symbol_at_exchange: str) -> dict[str, Any]:
        return self._request("GET", f"/board/{symbol_at_exchange}", None, retry_auth=True)

    def wallet_future(self, symbol_at_exchange: str | None = None) -> dict[str, Any]:
        endpoint = "/wallet/future" if symbol_at_exchange is None else f"/wallet/future/{symbol_at_exchange}"
        return self._request("GET", endpoint, None, retry_auth=True)

    def positions(self, **query: Any) -> dict[str, Any]:
        suffix = f"?{urlencode(query)}" if query else ""
        return self._request("GET", f"/positions{suffix}", None, retry_auth=True)

    def orders(self, **query: Any) -> dict[str, Any]:
        suffix = f"?{urlencode(query)}" if query else ""
        return self._request("GET", f"/orders{suffix}", None, retry_auth=True)

    def sendorder_future(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/sendorder/future", payload)

    def cancelorder(self, order_id: str, password: str | None = None) -> dict[str, Any]:
        return self._request("PUT", "/cancelorder", {"OrderID": order_id, "Password": password or self.password})

    def apisoftlimit(self) -> dict[str, Any]:
        return self._request("GET", "/apisoftlimit", None, retry_auth=True)

    def websocket_url(self) -> str:
        return self.base_url.replace("http://", "ws://").replace("https://", "wss://") + "/websocket"

    def websocket_base_url(self) -> str:
        root = self.base_url.removesuffix("/kabusapi")
        return root.replace("http://", "ws://").replace("https://", "wss://") + "/kabusapi/websocket"

    def _request(
        self,
        method: str,
        endpoint: str,
        body: dict[str, Any] | None,
        auth: bool = True,
        retry_auth: bool = False,
    ) -> dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if auth:
            if not self.token:
                raise KabuApiError("Client is not authenticated", category="auth_error")
            headers["X-API-KEY"] = self.token
        data = json.dumps(body).encode("utf-8") if body is not None else None
        request = Request(self.base_url + endpoint, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=10) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            if auth and retry_auth and exc.code == 401:
                try:
                    self.authenticate()
                    return self._request(method, endpoint, body, auth=auth, retry_auth=False)
                except KabuApiError as retry_exc:
                    if classify_kabu_api_error(retry_exc) == "auth_error":
                        raise KabuApiError(
                            f"auth_recovery_failed after kabu API HTTP 401: {details}",
                            status_code=401,
                            details=details,
                            category="auth_recovery_failed",
                        ) from retry_exc
                    raise
            raise KabuApiError(
                f"kabu API HTTP {exc.code}: {details}",
                status_code=exc.code,
                details=details,
                category=classify_kabu_api_error(f"kabu API HTTP {exc.code}: {details}"),
            ) from exc
        if not raw:
            return {}
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
        return {"data": parsed}


def extract_symbol_code(symbol_response: dict[str, Any], future_code: str) -> str:
    symbol = symbol_response.get("Symbol")
    if not isinstance(symbol, str) or not symbol:
        raise KabuApiError(f"Symbol was not returned for FutureCode={future_code}: {symbol_response}")
    return symbol


def build_future_registration_symbols(
    client: KabuStationClient,
    future_codes: list[str],
    deriv_month: int,
    exchanges: list[int],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    symbols: list[dict[str, Any]] = []
    resolved: list[dict[str, Any]] = []
    for future_code in future_codes:
        response = client.symbolname_future(future_code, deriv_month)
        symbol = extract_symbol_code(response, future_code)
        resolved.append(
            {
                "FutureCode": future_code,
                "DerivMonth": deriv_month,
                "Symbol": symbol,
                "SymbolName": response.get("SymbolName"),
                "DisplayName": response.get("DisplayName"),
            }
        )
        for exchange in exchanges:
            symbols.append({"Symbol": symbol, "Exchange": exchange})
    return symbols, resolved
