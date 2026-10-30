#!/usr/bin/env bash
set -euo pipefail

# Usage: bash scripts/build-release.sh vX.Y.Z [empty-output-directory]
tag=${1:?Usage: build-release.sh TAG [OUTPUT_DIRECTORY]}
if [[ ! "$tag" =~ ^v[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
  printf 'Invalid release tag: %s\n' "$tag" >&2
  exit 1
fi
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output=${2:-"$root/dist/$tag"}
mkdir -p "$output"
output=$(cd "$output" && pwd)
if [[ -n "$(ls -A "$output")" ]]; then
  printf 'Output directory must be empty: %s\n' "$output" >&2
  exit 1
fi
for tool in go tar gzip zip; do
  command -v "$tool" >/dev/null
done
if command -v sha256sum >/dev/null; then
  checksum=(sha256sum)
else
  command -v shasum >/dev/null
  checksum=(shasum -a 256)
fi
cd "$root"
commit=unknown
if command -v git >/dev/null && revision=$(git rev-parse --verify HEAD 2>/dev/null); then
  commit=$revision
  if [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then
    commit="$commit-dirty"
  fi
fi
for document in README.md LICENSE conf/hmq.config conf/cluster.md "docs/releases/$tag.md"; do
  test -s "$document"
done
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64; do
  os=${target%/*}
  arch=${target#*/}
  name="hmq_${tag}_${os}_${arch}"
  package="$stage/$name"
  mkdir -p "$package/conf"
  binary=hmq
  if [[ "$os" == windows ]]; then
    binary=hmq.exe
  fi
  printf 'Building %s\n' "$target"
  CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" \
    go build -mod=readonly -trimpath -buildvcs=false -ldflags="-s -w -X main.version=$tag -X main.commit=$commit" -o "$package/$binary" .
  cp README.md "$package/README.md"
  cp LICENSE "$package/LICENSE"
  mkdir -p "$package/docs/releases"
  cp "docs/releases/$tag.md" "$package/docs/releases/"
  cp conf/hmq.config conf/cluster.md "$package/conf/"
  # Preserve relative documentation links without copying runtime data or keys.
  while IFS= read -r -d '' document; do
    mkdir -p "$package/$(dirname "$document")"
    cp "$document" "$package/$document"
  done < <(find broker -type f -name '*.md' -print0)
  # Stable archive timestamps also make retrying the same build predictable.
  find "$package" -exec touch -t 200001010000.00 {} +
  if [[ "$os" == windows ]]; then
    (cd "$stage" && TZ=UTC zip -q -X -r "$output/$name.zip" "$name")
  else
    (cd "$stage" && tar -cf - "$name") | gzip -n > "$output/$name.tar.gz"
  fi
done
(
  cd "$output"
  "${checksum[@]}" ./*.tar.gz ./*.zip > SHA256SUMS
)
printf 'Release artifacts: %s\n' "$output"
