#!/usr/bin/env bash
# Classify every dependabot/* branch as SUPERSEDED (<= go.mod) or ACTIONABLE (> go.mod).
# Output TSV: <status>\t<branch>\t<branch-version>\t<go.mod-version>
#
# NON-DESTRUCTIVE: this script only reads. It does not delete or merge anything.
# Use its output to decide which dependabot branches are safe to delete (SUPERSEDED)
# and which are inputs to the conservative dependency-upgrade plan (ACTIONABLE).
#
# Tolerant of transient errors: a failing `go list` for one branch must not abort
# the whole run, so errexit is intentionally OFF (only nounset + pipefail).
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

# -mod=mod is required because the repo vendors dependencies (vendor/ exists);
# in vendor mode `go list` refuses to consult the module graph / proxy.
list_versions() { go list -mod=mod -m all 2>/dev/null; }

for branch in $(git branch -r --list 'origin/dependabot/*' | sed 's#^[[:space:]]*##'); do
  # name looks like: github.com/ipfs/kubo-0.41.0  or  github.com/redis/go-redis/v9-9.20.0
  name="${branch#origin/dependabot/go_modules/}"
  ver="${name##*-}"                       # 0.41.0 / 9.20.0
  mod="${name%-*}"                         # github.com/ipfs/kubo
  # Match the go.mod version for this module (handles /vN suffix).
  current="$(list_versions | awk -v m="$mod" '$1==m{print $2; exit}' || true)"
  current="${current#v}"
  if [ -z "$current" ]; then
    printf 'UNKNOWN\t%s\t%s\t(not in module graph)\n' "$branch" "$ver"
  elif printf '%s\n%s\n' "$ver" "$current" | sort -V -C; then
    # ver <= current  =>  branch version is not newer
    printf 'SUPERSEDED\t%s\t%s\t%s\n' "$branch" "$ver" "$current"
  else
    printf 'ACTIONABLE\t%s\t%s\t%s\n' "$branch" "$ver" "$current"
  fi
done
