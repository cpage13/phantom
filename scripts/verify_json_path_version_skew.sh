#!/usr/bin/env bash
# Reproduce the SQLite quoted-JSON-path version skew LOCALLY, on any machine.
#
# WHY THIS EXISTS. The repository already has a CI job
# (`unit-phantom-old-sqlite` in .github/workflows/per_pr.yml) that builds an old
# libsqlite3 and runs the special-character KVS tests against it. That job is
# the only thing standing between this defect class and a silent regression,
# and until now it was ALSO the only way to observe the defect at all: a
# developer machine running a modern SQLite cannot see it, the queries simply
# work. So the proof lived somewhere no one could run on demand, which is a
# poor place for the proof of a bug that has already recurred once.
#
# THE DEFECT. A JSON path label carrying a double quote has to be written as a
# quoted label with the quote escaped. SQLite below 3.50 cannot parse that, and
# it does NOT raise: `json_extract` returns NULL. A lookup built that way
# therefore reports the row ABSENT, with no error, for as long as the
# deployment runs. The fix is to address the key through `json_each` and a
# bound parameter, which every supported version parses.
#
# This script builds SQLite 3.43.2, a known-breaking version, and asserts both
# halves: that the legacy form silently answers NULL, and that the shipped
# `json_each` form answers correctly. It needs a compiler and network access,
# takes about a minute, and touches nothing in the repository.
#
# Usage:  bash scripts/verify_json_path_version_skew.sh
set -euo pipefail

VERSION_LABEL="3.43.2"
ARCHIVE="sqlite-autoconf-3430200"
URL="https://www.sqlite.org/2023/${ARCHIVE}.tar.gz"
WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

echo "Building SQLite ${VERSION_LABEL} in ${WORKDIR} ..."
curl -sSfL -o "${WORKDIR}/sqlite.tar.gz" "$URL"
tar -xzf "${WORKDIR}/sqlite.tar.gz" -C "$WORKDIR"
cc -O0 -DSQLITE_ENABLE_JSON1 -o "${WORKDIR}/sqlite_old" \
   "${WORKDIR}/${ARCHIVE}/shell.c" "${WORKDIR}/${ARCHIVE}/sqlite3.c" \
   -lpthread -lm

# ``:memory:`` is REQUIRED here. The sqlite3 shell reads its first positional
# argument as a DATABASE FILENAME, so passing the SQL alone silently opens a
# database named "select sqlite_version();" and prints nothing, which reads
# exactly like a failed build.
loaded="$("${WORKDIR}/sqlite_old" :memory: 'select sqlite_version();')"
echo "built sqlite_version = ${loaded}"
case "$loaded" in
  3.4*|3.3*|3.2*) ;;
  *) echo "PRECONDITION FAILED: built ${loaded}, which is not a breaking build."; exit 1 ;;
esac

# The legacy form: an escaped quote inside a quoted JSON-path label.
legacy="$(printf '.nullvalue NULLRESULT\nSELECT json_extract('"'"'{"a\\"b":1}'"'"', '"'"'$."a\\"b"'"'"');\n' \
  | "${WORKDIR}/sqlite_old" :memory:)"
# The shipped form: address the key through json_each and a bound value.
fixed="$(printf '.nullvalue NULLRESULT\nSELECT je.value FROM json_each('"'"'{"a\\"b":1}'"'"') je WHERE je.key = '"'"'a"b'"'"';\n' \
  | "${WORKDIR}/sqlite_old" :memory:)"

echo "legacy quoted-label form -> ${legacy}"
echo "shipped json_each form   -> ${fixed}"

if [ "$legacy" != "NULLRESULT" ]; then
  echo "UNEXPECTED: the legacy form did not answer NULL on ${loaded}."
  echo "Either this build is not actually affected, or the reproduction drifted."
  exit 1
fi
if [ "$fixed" != "1" ]; then
  echo "FAILED: the shipped json_each form does not work on ${loaded}."
  echo "This is the regression the CI gate exists to catch."
  exit 1
fi

echo
echo "CONFIRMED on SQLite ${loaded}:"
echo "  the legacy quoted-label form answers NULL SILENTLY, which is how a"
echo "  lookup came to report a chain absent for the life of a deployment;"
echo "  the shipped json_each form answers correctly."
