import json
import os
from urllib.error import HTTPError
from urllib.request import Request, urlopen

repository = os.environ["CLEANUP_REPOSITORY"]
branch = os.environ["CLEANUP_BRANCH"]
source_sha = os.environ["CLEANUP_SOURCE_SHA"]
merged_sha = os.environ["CLEANUP_MERGED_SHA"]
ci_id = int(os.environ["CLEANUP_CI_ID"])
runs = json.loads(os.environ["CLEANUP_RUNS"])
root = "https://api.github.com/repos/" + repository
assert repository in ("krwizxp/srg", "krwizxp/fcupdater")
assert branch == "codex/goal-round3-20260920"
assert os.environ["GITHUB_REPOSITORY"] == repository
assert os.environ["GITHUB_REF"] == "refs/heads/main"

def api(method, path, missing=False):
    request = Request(root + "/" + path, method=method, headers={
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer " + os.environ["GH_TOKEN"],
        "X-GitHub-Api-Version": "2026-03-10",
        "User-Agent": "goal-round3-cleanup",
    })
    try:
        with urlopen(request, timeout=30) as response:
            data = response.read()
            if method == "DELETE":
                assert response.status == 204
            return json.loads(data) if data else None
    except HTTPError as error:
        if missing and error.code == 404:
            return None
        raise RuntimeError(f"{method} {path}: HTTP {error.code}") from None

main_sha = api("GET", "git/ref/heads/main")["object"]["sha"]
assert main_sha == os.environ["GITHUB_SHA"], "Main moved before cleanup"
for ancestor in (source_sha, merged_sha):
    comparison = api("GET", "compare/" + ancestor + "..." + main_sha)
    assert comparison["merge_base_commit"]["sha"] == ancestor
    assert comparison["behind_by"] == 0
ci = api("GET", f"actions/runs/{ci_id}")
assert (ci["name"], ci["head_branch"], ci["head_sha"], ci["status"], ci["conclusion"]) == (
    "CI", "main", merged_sha, "completed", "success")
source_path = "git/ref/heads/" + branch
source = api("GET", source_path, missing=True)
assert source is None or source["object"]["sha"] == source_sha
pending = []
for run_id, sha in runs:
    run = api("GET", f"actions/runs/{run_id}", missing=True)
    if run is not None:
        assert (run["head_branch"], run["head_sha"], run["status"]) == (branch, sha, "completed")
        pending.append(run_id)
assert api("GET", "git/ref/heads/main")["object"]["sha"] == main_sha
source = api("GET", source_path, missing=True)
if source is not None:
    assert source["object"]["sha"] == source_sha
    api("DELETE", "git/refs/heads/" + branch)
    print("Deleted verified temporary branch:", branch, flush=True)
for run_id in pending:
    api("DELETE", f"actions/runs/{run_id}")
    print("Deleted temporary run and artifacts:", run_id, flush=True)
assert api("GET", source_path, missing=True) is None
for run_id, _ in runs:
    assert api("GET", f"actions/runs/{run_id}", missing=True) is None
summary = f"Verified branch absent: {branch}\n\nVerified temporary runs absent: {len(runs)}\n"
with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as output:
    output.write(summary)
print(summary, flush=True)
