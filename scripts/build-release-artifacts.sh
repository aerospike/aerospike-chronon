#!/usr/bin/env bash
# Builds the release artifacts for every Scala version in crossScalaVersions into
# one flat directory (default: dist/), for the JFrog deploy in .github/workflows/release.yml:
#   aerospike-chronon_<scala>-<version>.{jar,pom}, -sources.jar, -javadoc.jar
#       the library, with its dependencies declared in the POM
#   aerospike-chronon-assembly_<scala>-<version>.{jar,pom}, -sources.jar, -javadoc.jar
#       the online jar Chronon loads (--online-jar). Its POM declares no
#       dependencies because the jar already carries them. Maven Central requires
#       sources and javadoc for every artifact, so it reuses the library's.
# <version> overrides ThisBuild / version in build.sbt, so a dev build can carry a
# -dev.N version without editing the build.
#
# Usage: scripts/build-release-artifacts.sh <version> [out-dir]
set -euo pipefail

VERSION="${1:?usage: scripts/build-release-artifacts.sh <version> [out-dir]}"
OUT_DIR="${2:-dist}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

# GitHub's ubuntu-24.04 image no longer ships sbt.
if ! command -v sbt >/dev/null 2>&1; then
  sbt_version="$(sed -n 's/^sbt\.version=//p' project/build.properties)"
  sbt_home="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/sbt-${sbt_version}"
  if [[ ! -x "${sbt_home}/sbt/bin/sbt" ]]; then
    echo "==> Installing sbt ${sbt_version}"
    mkdir -p "${sbt_home}"
    curl -fsSL "https://github.com/sbt/sbt/releases/download/v${sbt_version}/sbt-${sbt_version}.tgz" \
      | tar -xz -C "${sbt_home}"
  fi
  export PATH="${sbt_home}/sbt/bin:${PATH}"
fi

M2_DIR="${HOME}/.m2/repository/com/aerospike"

echo "==> Building aerospike-chronon ${VERSION}"
rm -rf "${M2_DIR}"/aerospike-chronon_*/"${VERSION}"
sbt --batch "set ThisBuild / version := \"${VERSION}\"" +clean +publishM2 +assembly

bash scripts/check-assembly.sh

mkdir -p "${OUT_DIR}"

shopt -s nullglob
module_dirs=("${M2_DIR}"/aerospike-chronon_*/"${VERSION}")
shopt -u nullglob
if [[ ${#module_dirs[@]} -eq 0 ]]; then
  echo "ERROR: publishM2 produced nothing under ${M2_DIR}/aerospike-chronon_*/${VERSION}" >&2
  exit 1
fi

for module_dir in "${module_dirs[@]}"; do
  artifact_id="$(basename "$(dirname "${module_dir}")")"
  scala_binary="${artifact_id#aerospike-chronon_}"
  stem="${artifact_id}-${VERSION}"

  echo "==> Collecting ${artifact_id}"
  for file in "${stem}.jar" "${stem}.pom" "${stem}-sources.jar" "${stem}-javadoc.jar"; do
    if [[ ! -f "${module_dir}/${file}" ]]; then
      echo "ERROR: ${module_dir}/${file} is missing" >&2
      exit 1
    fi
    # Matched with case rather than piping to grep -q: under pipefail, grep -q exits on
    # the first match, unzip takes SIGPIPE, and a match reads as a failed pipeline.
    if [[ "${file}" == *.jar ]]; then
      entries="$(unzip -Z1 "${module_dir}/${file}")"
      case "${entries}" in
        *"META-INF/maven/com.aerospike/${artifact_id}/pom.properties"*) ;;
        *)
          echo "ERROR: ${file} has no META-INF/maven/com.aerospike/${artifact_id}/pom.properties" >&2
          exit 1
          ;;
      esac
    fi
    cp "${module_dir}/${file}" "${OUT_DIR}/"
  done

  assembly_id="aerospike-chronon-assembly_${scala_binary}"
  assembly_jar="target/scala-${scala_binary}/aerospike-chronon-assembly-${VERSION}.jar"
  if [[ ! -f "${assembly_jar}" ]]; then
    echo "ERROR: ${assembly_jar} is missing" >&2
    exit 1
  fi
  echo "==> Collecting ${assembly_id}"
  cp "${assembly_jar}" "${OUT_DIR}/${assembly_id}-${VERSION}.jar"
  # The library POM's first <artifactId> is the project's own. Reusing it keeps the
  # name, license, SCM and developer metadata identical across the two artifacts.
  sed -e "0,/<artifactId>${artifact_id}<\/artifactId>/s//<artifactId>${assembly_id}<\/artifactId>/" \
    -e '/<dependencies>/,/<\/dependencies>/d' \
    "${module_dir}/${stem}.pom" >"${OUT_DIR}/${assembly_id}-${VERSION}.pom"

  # The JFrog deploy takes coordinates from pom.properties, so the copies must name
  # the assembly or they would be filed under the library.
  props_dir="$(mktemp -d)"
  mkdir -p "${props_dir}/META-INF/maven/com.aerospike/${assembly_id}"
  printf 'groupId=com.aerospike\nartifactId=%s\nversion=%s\n' "${assembly_id}" "${VERSION}" \
    >"${props_dir}/META-INF/maven/com.aerospike/${assembly_id}/pom.properties"
  for classifier in sources javadoc; do
    out_jar="$(cd "${OUT_DIR}" && pwd)/${assembly_id}-${VERSION}-${classifier}.jar"
    cp "${module_dir}/${stem}-${classifier}.jar" "${out_jar}"
    zip -qd "${out_jar}" "META-INF/maven/*"
    (cd "${props_dir}" && zip -qr "${out_jar}" META-INF/maven)
  done
  rm -rf "${props_dir}"
done

echo "==> Artifacts emitted to ${OUT_DIR}:"
ls -1 "${OUT_DIR}"
