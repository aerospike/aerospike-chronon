#!/bin/sh
# Lifecycle regression cases for docker/gen-tls-certs.sh. Runs inside the
# pinned eclipse-temurin image (see docker-compose.yml tls-certs-init) so the
# openssl/keytool under test is the same one setup uses:
#
#   docker run --rm -v "$PWD:/repo" -w /repo \
#     eclipse-temurin:21-jdk sh docker/test-tls-fixture.sh
#
# Each case works in a throwaway AEROSPIKE_TLS_CERT_DIR so a failed run never
# touches real fixture material.
#
# TLS_LIVE=1 instead runs the live provisioning cases against the real
# tls-profile fixtures from the aerospike-tools image on the fixture network.
set -u

SCRIPT="${TLS_INIT_SCRIPT:-docker/gen-tls-certs.sh}"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
pass=0
failed=""

ok()   { pass=$((pass + 1)); echo "ok   - $1"; }
bad()  { failed="$failed $1"; echo "FAIL - $1"; }
check() {
  if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected [$2], got [$3])"; fi
}

run_init() {
  OUT=$(AEROSPIKE_TLS_CERT_DIR="$1" sh "$SCRIPT" 2>&1)
  return $?
}

fingerprints() {
  for f in "$1"/*.crt; do openssl x509 -in "$f" -outform DER 2>/dev/null | openssl dgst -sha256; done | openssl dgst -sha256 | awk '{print $2}'
}

FILES="ca.crt ca.key server.crt server.key client.crt client.key other-ca.crt other-ca.key untrusted-client.crt untrusted-client.key other-user.crt other-user.key"
STORES="ca.truststore.p12 other-ca.truststore.p12 client.keystore.p12 untrusted.keystore.p12 other-user.keystore.p12 storepass.txt"

if [ "${TLS_LIVE:-0}" != 1 ]; then

d="$WORK/fresh"
run_init "$d" && rc=0 || rc=$?
check "fresh generation succeeds" 0 "$rc"
missing=0
for f in $FILES $STORES .complete; do [ -f "$d/$f" ] || missing=$((missing + 1)); done
check "fresh generation emits all files + .complete" 0 "$missing"
perm=$(stat -c %a "$d/client.key" 2>/dev/null || stat -f %Lp "$d/client.key")
check "private keys are owner-only" 600 "$perm"
n=$(ls "$d/server" | wc -l | tr -d ' ')
check "server-only dir has exactly the server material" 3 "$n"

fp1=$(fingerprints "$d")
run_init "$d" && rc=0 || rc=$?
check "re-run on valid set succeeds" 0 "$rc"
check "fingerprints unchanged on reuse" "$fp1" "$(fingerprints "$d")"

d="$WORK/legacy"; mkdir -p "$d"
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout "$d/ca.key" -out "$d/ca.crt" -subj "/CN=chronon-test-ca" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout "$d/server.key" -out "$d/s.csr" -subj "/CN=aerospike-tls" 2>/dev/null
printf "subjectAltName=DNS:aerospike-tls,IP:127.0.0.1" > "$d/s.ext"
openssl x509 -req -in "$d/s.csr" -days 3650 -CA "$d/ca.crt" -CAkey "$d/ca.key" -CAcreateserial -out "$d/server.crt" -extfile "$d/s.ext" 2>/dev/null
for id in client:chronon-client other-user:not-a-provisioned-user; do
  f=${id%%:*}; cn=${id#*:}
  openssl req -newkey rsa:2048 -nodes -keyout "$d/$f.key" -out "$d/$f.csr" -subj "/CN=$cn" 2>/dev/null
  openssl x509 -req -in "$d/$f.csr" -days 3650 -CA "$d/ca.crt" -CAkey "$d/ca.key" -CAcreateserial -out "$d/$f.crt" 2>/dev/null
done
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout "$d/other-ca.key" -out "$d/other-ca.crt" -subj "/CN=untrusted-ca" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout "$d/untrusted-client.key" -out "$d/u.csr" -subj "/CN=untrusted-client" 2>/dev/null
openssl x509 -req -in "$d/u.csr" -days 3650 -CA "$d/other-ca.crt" -CAkey "$d/other-ca.key" -CAcreateserial -out "$d/untrusted-client.crt" 2>/dev/null
printf "storepass" > "$d/storepass.txt"
keytool -importcert -noprompt -storetype PKCS12 -alias chronon-test-ca -keystore "$d/ca.truststore.p12" -file "$d/ca.crt" -storepass storepass 2>/dev/null
keytool -importcert -noprompt -storetype PKCS12 -alias untrusted-ca -keystore "$d/other-ca.truststore.p12" -file "$d/other-ca.crt" -storepass storepass 2>/dev/null
for id in client:client.crt:ca.crt:client.key:chronon-client untrusted:untrusted-client.crt:other-ca.crt:untrusted-client.key:untrusted-client other-user:other-user.crt:ca.crt:other-user.key:not-a-provisioned-user; do
  name=${id%%:*}; rest=${id#*:}
  crt=${rest%%:*}; rest=${rest#*:}; ca=${rest%%:*}; rest=${rest#*:}; key=${rest%%:*}; alias=${rest#*:}
  cat "$d/$crt" "$d/$ca" "$d/$key" > "$d/$name.chain.pem"
  openssl pkcs12 -export -in "$d/$name.chain.pem" -out "$d/$name.keystore.p12" -name "$alias" -passout pass:storepass 2>/dev/null
done
rm -f "$d"/*.csr "$d"/*.srl "$d"/*.ext "$d"/*.chain.pem
chmod 644 "$d"/*
touch "$d/.complete"
fp1=$(fingerprints "$d")
run_init "$d" && rc=0 || rc=$?
check "legacy set adopted" 0 "$rc"
check "legacy fingerprints preserved" "$fp1" "$(fingerprints "$d")"
check "legacy keys tightened to owner-only" 600 "$(stat -c %a "$d/ca.key" 2>/dev/null || stat -f %Lp "$d/ca.key")"
[ -f "$d/server/server.key" ] && ok "legacy set gains server-only dir" || bad "legacy set gains server-only dir"

reject() {
  run_init "$2" && rc=0 || rc=$?
  if [ "$rc" -ne 0 ] && printf '%s' "$OUT" | grep -q "$3"; then ok "$1"; else bad "$1 (rc=$rc out=$OUT)"; fi
}

d="$WORK/missing"; cp -r "$WORK/fresh" "$d" 2>/dev/null || { mkdir "$d"; }
rm "$d/client.key"
reject "missing file rejected" "$d" "Missing or unsafe"

d="$WORK/mismatch"; cp -r "$WORK/fresh" "$d"
cp "$d/other-user.key" "$d/client.key"
reject "cert/key mismatch rejected" "$d" "mismatch"

d="$WORK/expired"; cp -r "$WORK/fresh" "$d"
keytool -genkeypair -alias expired -keystore "$d/expired.p12" -storetype PKCS12 \
  -storepass storepass -keypass storepass -keyalg RSA -keysize 2048 \
  -dname "CN=chronon-test-ca" -startdate "2020/01/01 00:00:00" -validity 1 \
  -ext bc:c 2>/dev/null
keytool -exportcert -rfc -alias expired -keystore "$d/expired.p12" \
  -storepass storepass -file "$d/ca.crt" 2>/dev/null
rm "$d/expired.p12"
reject "expired certificate rejected" "$d" "expired\|Invalid"

d="$WORK/interrupted"; mkdir "$d"; mkdir "$d/.staging"
reject "interrupted generation rejected" "$d" "[Ii]nterrupted"

d="$WORK/locked"; mkdir "$d"; mkdir "$d/.init-lock"
reject "concurrent initializer rejected" "$d" "lock"

d="$WORK/trusted-untrusted"; cp -r "$WORK/fresh" "$d"
openssl req -new -key "$d/untrusted-client.key" -out "$d/u.csr" -subj "/CN=untrusted-client" 2>/dev/null
printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=clientAuth\n' > "$d/u.ext"
openssl x509 -req -in "$d/u.csr" -days 30 -CA "$d/ca.crt" -CAkey "$d/ca.key" -CAcreateserial -out "$d/untrusted-client.crt" -extfile "$d/u.ext" 2>/dev/null
rm -f "$d/u.csr" "$d/u.ext" "$d"/*.srl
reject "CA-signed untrusted identity rejected" "$d" "unexpectedly trusted"

d="$WORK/stale-store"; cp -r "$WORK/fresh" "$d"
cp "$d/other-ca.truststore.p12" "$d/ca.truststore.p12"
reject "stale truststore rejected" "$d" "store\|truststore\|mismatch\|Inconsistent"

d="$WORK/extra-file"; cp -r "$WORK/fresh" "$d"
touch "$d/stray.pem"
reject "unexpected entry rejected" "$d" "Unexpected"

d="$WORK/server-drift"; cp -r "$WORK/fresh" "$d"
cp "$d/client.crt" "$d/server/server.crt"
reject "inconsistent server material rejected" "$d" "[Ii]nconsistent"

d="$WORK/bad-storepass"; cp -r "$WORK/fresh" "$d"
printf "wrong" > "$d/storepass.txt"
reject "wrong storepass rejected" "$d" "storepass\|password"

fi

if [ "${TLS_LIVE:-0}" = 1 ]; then
  PROV="${TLS_PROV_SCRIPT:-docker/provision-tls-users.sh}"
  CERTS="${AEROSPIKE_TLS_CERT_DIR:-docker/tls-certs}"

  for host in aerospike-tls aerospike-tls-mtls; do
    OUT=$(AEROSPIKE_TLS_CERT_DIR="$CERTS" AEROSPIKE_TLS_HOST="$host" sh "$PROV" 2>&1) && rc=0 || rc=$?
    check "idempotent provisioning on $host" 0 "$rc"

    asadm --no-config-file --no-color --single-node --host "$host" --port 4333 \
      --user admin --password admin --tls-enable --tls-name aerospike-tls \
      --tls-cafile "$CERTS/ca.crt" --tls-certfile "$CERTS/client.crt" \
      --tls-keyfile "$CERTS/client.key" \
      -e "enable; manage acl grant user chronon-user roles sys-admin" >/dev/null 2>&1
    OUT=$(AEROSPIKE_TLS_CERT_DIR="$CERTS" AEROSPIKE_TLS_HOST="$host" sh "$PROV" 2>&1) && rc=0 || rc=$?
    if [ "$rc" -ne 0 ]; then ok "role drift rejected on $host"; else bad "role drift rejected on $host"; fi
    asadm --no-config-file --no-color --single-node --host "$host" --port 4333 \
      --user admin --password admin --tls-enable --tls-name aerospike-tls \
      --tls-cafile "$CERTS/ca.crt" --tls-certfile "$CERTS/client.crt" \
      --tls-keyfile "$CERTS/client.key" \
      -e "enable; manage acl revoke user chronon-user roles sys-admin" >/dev/null 2>&1
    OUT=$(AEROSPIKE_TLS_CERT_DIR="$CERTS" AEROSPIKE_TLS_HOST="$host" sh "$PROV" 2>&1) && rc=0 || rc=$?
    check "provisioning recovers after drift cleanup on $host" 0 "$rc"
  done

  OUT=$(AEROSPIKE_TLS_CERT_DIR="$CERTS" AEROSPIKE_TLS_HOST=aerospike-tls AEROSPIKE_TLS_PORT=4399 AEROSPIKE_TLS_WAIT_ATTEMPTS=2 AEROSPIKE_TLS_WAIT_DELAY=1 sh "$PROV" 2>&1) && rc=0 || rc=$?
  if [ "$rc" -ne 0 ]; then ok "unreachable server fails bounded"; else bad "unreachable server fails bounded"; fi
fi

echo
echo "$pass passed; failed:${failed:-none}"
[ -z "$failed" ]
