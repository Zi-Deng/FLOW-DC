"""Exact UTF-8 GitHub JSON transport, without terminal rendering of body strings."""

import hashlib
import io
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import zipfile

from workflow import WorkflowError


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Never forward an authorization header based on an API redirect.
        return None


def api(repository, suffix, *, token_source, data=None, paginate=False, method=None, page_key=None):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise WorkflowError("Invalid GitHub repository identity")
    prefix = f"https://api.github.com/repos/{repository}/"

    def valid_url(url):
        parsed = urllib.parse.urlsplit(url)
        if (
            not url.startswith(prefix)
            or parsed.scheme != "https"
            or parsed.netloc != "api.github.com"
            or parsed.fragment
            or any(part in {".", ".."} for part in urllib.parse.unquote(parsed.path).split("/"))
            or any(ord(c) < 32 for c in url)
            or "\\" in url
        ):
            raise WorkflowError("GitHub pagination URL escaped its authenticated repository origin")

    url = prefix + suffix
    valid_url(url)
    endpoint = urllib.parse.urlsplit(url).path
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or token_source()
    if not isinstance(token, str) or not token.strip() or any(c in token for c in "\r\n"):
        raise WorkflowError("GitHub authentication unavailable")
    headers = {
        "Authorization": f"Bearer {token.strip()}",
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "agentic-review-exact-transport",
    }
    encoded = json.dumps(data, ensure_ascii=False).encode("utf-8") if data is not None else None
    method = method or ("POST" if data is not None else "GET")
    if paginate and (method != "GET" or data is not None):
        raise WorkflowError("Pagination is only supported for read-only GitHub requests")
    opener = urllib.request.build_opener(NoRedirect())
    seen, items, total = set(), [], 0
    while url:
        valid_url(url)
        if urllib.parse.urlsplit(url).path != endpoint:
            raise WorkflowError("GitHub pagination changed its authenticated endpoint")
        if url in seen or len(seen) >= 1000:
            raise WorkflowError("GitHub pagination cycle or page limit reached")
        seen.add(url)
        request = urllib.request.Request(url, data=encoded, headers=headers, method=method)
        try:
            with opener.open(request, timeout=120) as response:
                raw = response.read(16000001)
                link = response.headers.get("Link", "")
            total += len(raw)
            if len(raw) > 16000000 or total > 64000000:
                raise WorkflowError("GitHub response budget exceeded; no truncated evidence accepted")
            result = json.loads(raw.decode("utf-8")) if raw else None
        except urllib.error.HTTPError as exc:
            raise WorkflowError(f"GitHub API HTTP {exc.code}; response body withheld") from None
        except (urllib.error.URLError, OSError, ValueError):
            raise WorkflowError(
                "GitHub API transport or JSON decoding failed; sensitive details withheld"
            ) from None
        if not paginate:
            return result
        page = result.get(page_key) if page_key and isinstance(result, dict) else result
        if not isinstance(page, list):
            raise WorkflowError("Unexpected GitHub pagination shape")
        items.extend(page)
        next_links = re.findall(r'<([^>]+)>;\s*rel="next"', link)
        if len(next_links) > 1:
            raise WorkflowError("Ambiguous GitHub pagination links")
        url = next_links[0] if next_links else None
    return items


def artifact_receipt(repository, artifact_id, token_source):
    """Fetch one bounded Actions ZIP; never forward authentication to blob storage."""
    if (
        not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository)
        or type(artifact_id) is not int
        or artifact_id <= 0
    ):
        raise WorkflowError("Invalid validation artifact identity")
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or token_source()
    opener = urllib.request.build_opener(NoRedirect())
    url = f"https://api.github.com/repos/{repository}/actions/artifacts/{artifact_id}/zip"
    request = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    )
    try:
        try:
            response = opener.open(request, timeout=120)
        except urllib.error.HTTPError as exc:
            if exc.code != 302:
                raise
            location = exc.headers.get("Location", "")
            parsed = urllib.parse.urlsplit(location)
            host = parsed.hostname or ""
            if (
                parsed.scheme != "https"
                or parsed.port not in {None, 443}
                or parsed.username
                or parsed.password
                or not (
                    host.endswith(".blob.core.windows.net") or host.endswith(".actions.githubusercontent.com")
                )
            ):
                raise WorkflowError("Artifact download escaped approved HTTPS storage origin") from None
            # Deliberately construct a new request without any GitHub headers.
            response = opener.open(urllib.request.Request(location), timeout=120)
        with response:
            raw = response.read(2000001)
        if len(raw) > 2000000:
            raise WorkflowError("Validation artifact exceeds download budget")
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            members = archive.infolist()
            if len(members) != 1 or members[0].filename != "receipt.json" or members[0].file_size > 128000:
                raise WorkflowError("Unexpected validation artifact contents")
            value = json.loads(archive.read(members[0]).decode("utf-8"))
        return value, hashlib.sha256(raw).hexdigest()
    except (urllib.error.URLError, OSError, ValueError, zipfile.BadZipFile):
        raise WorkflowError("Validation artifact download/decoding unavailable; details withheld") from None
