#!/bin/sh
# Provisions users for the opt-in TLS fixture. Runs inside the Compose tls
# profile on the service network against both EE fixture servers —
# aerospike-tls (server auth) and aerospike-tls-mtls (client cert required).
#
# `tls-authenticate-client any` on the mTLS server requires every connection —
# including the admin bootstrap — to present a client certificate signed by the
# fixture CA; the script presents one on both servers so behavior is uniform.
# The client certificate's CN becomes the PKI Aerospike username; `nopassword`
# marks it as a PKI-only user. A second password-authenticated user covers the
# internal-auth path. Idempotent: users that already exist with the expected
# roles and auth mode are fine; drift or any other failure fails the script so
# a setup step can't silently skip provisioning.
#
# Tool flags verified against asadm/asinfo 5.0.2 (aerospike/aerospike-tools
# digest-pinned in docker-compose.yml). Recheck on any AEROSPIKE_TOOLS_IMAGE
# bump: `show users` must emit a `User|Roles|Auth Mode` table, and
# `asinfo --auth PKI` authenticates as the client certificate's CN with no
# --user flag.
set -eu

if [ -n "${AEROSPIKE_TLS_HOST:-}" ]; then
  HOSTS="$AEROSPIKE_TLS_HOST"
else
  HOSTS="${AEROSPIKE_TLS_HOSTS:-aerospike-tls aerospike-tls-mtls}"
fi
PORT="${AEROSPIKE_TLS_PORT:-4333}"
CERTS="${AEROSPIKE_TLS_CERT_DIR:-/certs}"
PKI_USER="${AEROSPIKE_TLS_PKI_USER:-chronon-client}"
PW_USER="${AEROSPIKE_TLS_PW_USER:-chronon-user}"
PW_PASS="${AEROSPIKE_TLS_PW_PASS:-chronon-pass}"
ROLES="read-write"
EXPECTED_ROLES="read-write"

fail() {
  echo "$*" >&2
  exit 1
}

asadm_tls() {
  timeout 15 asadm --no-config-file --no-color --single-node \
    --host "$HOST" --port "$PORT" --user admin --password admin \
    --tls-enable --tls-name aerospike-tls --tls-cafile "$CERTS/ca.crt" \
    --tls-certfile "$CERTS/client.crt" --tls-keyfile "$CERTS/client.key" \
    -e "$1"
}

asinfo_tls() {
  timeout 15 asinfo --no-config-file \
    --host "$HOST" --port "$PORT" \
    --tls-enable --tls-name aerospike-tls --tls-cafile "$CERTS/ca.crt" \
    --tls-certfile "$CERTS/client.crt" --tls-keyfile "$CERTS/client.key" \
    "$@" --value status
}

row() {
  printf '%s\n' "$users" | awk -F '|' -v user="$1" '
    { for (i=1;i<=NF;i++) gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i) }
    NF == 3 && $1 == "User" && $2 == "Roles" && $3 == "Auth Mode" { table=1; next }
    NF != 3 { table=0 }
    table && $1 == user { print $2 "|" $3 }
  '
}

inspect() {
  users=$(asadm_tls 'show users' 2>&1) || fail "Cannot inspect fixture users on $HOST: $users"
  headers=$(printf '%s\n' "$users" | awk -F '|' '
    { for (i=1;i<=NF;i++) gsub(/^[[:space:]]+|[[:space:]]+$/, "", $i) }
    NF == 3 && $1 == "User" && $2 == "Roles" && $3 == "Auth Mode" { count++ }
    END { print count+0 }
  ')
  [ "$headers" = 1 ] || fail "Unsupported asadm show users header on $HOST; expected User|Roles|Auth Mode exactly once: $users"
  [ -n "$(row admin)" ] || fail "Admin user inspection failed on $HOST: $users"
  [ -z "$(row not-a-provisioned-user)" ] || fail "Negative-test CN must remain unprovisioned on $HOST."
}

check_state() {
  entry=$(row "$1")
  auth=${entry##*|}
  roles=${entry%|*}
  [ "$auth" = "$2" ] || return 1
  [ "$(printf '%s' "$roles" | tr ',' '\n' | sort)" = "$(printf '%s' "$EXPECTED_ROLES" | tr ',' '\n' | sort)" ]
}

for HOST in $HOSTS; do
  attempt=0
  max_attempts="${AEROSPIKE_TLS_WAIT_ATTEMPTS:-90}"
  delay="${AEROSPIKE_TLS_WAIT_DELAY:-2}"
  case "$max_attempts" in ''|*[!0-9]*) fail "AEROSPIKE_TLS_WAIT_ATTEMPTS must be a positive integer: $max_attempts" ;; esac
  case "$delay" in ''|*[!0-9]*) fail "AEROSPIKE_TLS_WAIT_DELAY must be a positive integer: $delay" ;; esac
  [ "$max_attempts" -ge 1 ] || fail "AEROSPIKE_TLS_WAIT_ATTEMPTS must be a positive integer: $max_attempts"
  until users=$(asadm_tls 'show users' 2>&1); do
    attempt=$((attempt + 1))
    [ "$attempt" -lt "$max_attempts" ] || fail "TLS server $HOST did not accept admin authentication: $users"
    sleep "$delay"
  done

  inspect
  if [ -z "$(row "$PKI_USER")" ]; then
    output=$(asadm_tls "enable; manage acl create user $PKI_USER password nopassword roles $ROLES" 2>&1) || fail "Failed to provision PKI user on $HOST: $output"
    inspect
  fi
  if [ -z "$(row "$PW_USER")" ]; then
    output=$(asadm_tls "enable; manage acl create user $PW_USER password $PW_PASS roles $ROLES" 2>&1) || fail "Failed to provision password user on $HOST: $output"
    inspect
  fi

  check_state "$PKI_USER" "PKI" || fail "PKI user on $HOST must have exactly the $EXPECTED_ROLES roles and PKI-only authentication; found: $(row "$PKI_USER")"
  check_state "$PW_USER" "password,PKI" || fail "Password user on $HOST must have exactly the $EXPECTED_ROLES roles with password and PKI authentication; found: $(row "$PW_USER")"

  case " ${MTLS_HOSTS:=aerospike-tls-mtls} " in
    *" $HOST "*)
      status=$(asinfo_tls --auth PKI 2>&1) || fail "PKI authentication failed on $HOST: $status"
      [ "$status" = ok ] || fail "PKI authentication on $HOST returned unexpected status: $status"
      ;;
  esac
  status=$(asinfo_tls --auth INTERNAL --user "$PW_USER" --password "$PW_PASS" 2>&1) || fail "Password authentication failed on $HOST: $status"
  [ "$status" = ok ] || fail "Password authentication on $HOST returned unexpected status: $status"
  echo "$HOST: user state and password authentication verified."
done
echo "TLS fixture ready; run the host TLS IT suite for record write/read verification."
