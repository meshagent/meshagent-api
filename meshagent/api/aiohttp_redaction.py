from aiohttp.client_exceptions import ClientResponseError
from aiohttp.client_reqrep import RequestInfo

_REDACTED_URL = "<redacted>"
_PATCHED = False


def _redacted_request_info_repr(self: RequestInfo) -> str:
    return (
        f"RequestInfo(url={_REDACTED_URL!r}, method={self.method!r}, "
        "headers=<redacted>, real_url='<redacted>')"
    )


def _redacted_client_response_error_str(self: ClientResponseError) -> str:
    return f"{self.status}, message={self.message!r}, url='{_REDACTED_URL}'"


def _redacted_client_response_error_repr(self: ClientResponseError) -> str:
    return (
        f"ClientResponseError(status={self.status!r}, message={self.message!r}, "
        f"url='{_REDACTED_URL}')"
    )


def patch_aiohttp_url_redaction() -> None:
    global _PATCHED
    if _PATCHED:
        return

    RequestInfo.__repr__ = _redacted_request_info_repr
    RequestInfo.__str__ = _redacted_request_info_repr
    ClientResponseError.__str__ = _redacted_client_response_error_str
    ClientResponseError.__repr__ = _redacted_client_response_error_repr

    _PATCHED = True
