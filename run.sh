#!/usr/bin/env bash
# Small helper to run the SQL generator CLI with environment variables from .env
set -euo pipefail

# If a .env file exists, export its variables into the environment
if [ -f .env ]; then
  # shellcheck disable=SC1091
  set -a
  . .env
  set +a
fi

# Pick runner: prefer 'uv', fallback to 'python'
if command -v uv >/dev/null 2>&1; then
  RUNNER=(uv run)
else
  RUNNER=(python)
fi

# 🎨 Colors
RESET="\033[0m"
BOLD="\033[1m"
CYAN="\033[36m"
GREEN="\033[32m"
RED="\033[31m"
YELLOW="\033[33m"

# 🌟 Emojis
SQL_EMOJI="🗄️"
DB_EMOJI="🛢️"
SERVER_EMOJI="🌐"
ROCKET="🚀"
ERROR="❌"
CHECK="✅"

DEFAULT_TABLES="${TABLES_JSON:-data/tables.json}"
DEFAULT_MODEL="${OPENROUTER_MODEL:-moonshotai/kimi-k2:free}"
DEFAULT_DB_URL="${DB_URL:-}"
DEFAULT_HOST="${HOST:-127.0.0.1}"
DEFAULT_PORT="${PORT:-8000}"

echo -e "${CYAN}${BOLD}SQL Generator CLI Tool${RESET}"
echo -e "${YELLOW}-------------------------------------${RESET}"

choose_dialect() {
  # Print menu to stderr so command substitution only captures the selected value
  local allow_auto="${1:-false}"

  echo -e "${CYAN}Select target SQL dialect:${RESET}" >&2
  echo -e "1) Oracle" >&2
  echo -e "2) PostgreSQL" >&2
  echo -e "3) MySQL" >&2
  if [ "${allow_auto}" = "true" ]; then
    echo -e "4) Auto (infer from DB URL)" >&2
  fi

  local prompt="Enter your choice [1-3"
  if [ "${allow_auto}" = "true" ]; then prompt="${prompt}/4"; fi
  prompt="${prompt}]: "

  local dchoice
  read -r -p "${prompt}" dchoice

  case "${dchoice}" in
    1|"") printf '%s\n' "oracle" ;;
    2)    printf '%s\n' "postgresql" ;;
    3)    printf '%s\n' "mysql" ;;
    4)
      if [ "${allow_auto}" = "true" ]; then
        printf '%s\n' "auto"
      else
        printf '%s\n' "oracle"
      fi
      ;;
    *)
      echo -e "${YELLOW}Unknown choice, defaulting to Oracle.${RESET}" >&2
      printf '%s\n' "oracle"
      ;;
  esac
}

urlencode() {
  # URL-encode stdin using Python if available, otherwise return input as-is with a warning.
  if command -v python3 >/dev/null 2>&1; then
    python3 - <<'PY'
import sys, urllib.parse
s = sys.stdin.read().strip()
print(urllib.parse.quote(s, safe=''))
PY
  else
    echo -e "${YELLOW}Warning: python3 not found; not URL-encoding input.${RESET}" >&2
    cat
  fi
}

build_oracle_url() {
  # Interactively build a safe Oracle URL with URL-encoded credentials and Service Name or SID.
  echo -e "${CYAN}Build Oracle connection URL${RESET}"
  read -r -p "Username: " o_user
  read -r -s -p "Password (hidden): " o_pass; echo
  read -r -p "Host (e.g., 141.94.250.58): " o_host
  read -r -p "Port [1521]: " o_port; o_port=${o_port:-1521}

  echo -e "Connect using:"
  echo -e "  1) Service Name (recommended, e.g., XE, XEPDB1)"
  echo -e "  2) SID (legacy)"
  read -r -p "Choice [1-2]: " cs_choice
  if [ "${cs_choice}" = "2" ]; then
    read -r -p "SID (e.g., XE): " o_sid
    local_part="?sid=${o_sid}"
  else
    read -r -p "Service Name [XE]: " o_svc; o_svc=${o_svc:-XE}
    local_part="?service_name=${o_svc}"
  fi

  # URL-encode username and password
  enc_user="$(printf '%s' "$o_user" | urlencode)"
  enc_pass="$(printf '%s' "$o_pass" | urlencode)"

  echo "oracle+oracledb://${enc_user}:${enc_pass}@${o_host}:${o_port}/${local_part}"
}

ensure_oracle_client() {
  # If connecting to Oracle and thick mode env isn't set, prompt for Instant Client path.
  # This avoids DPY-3010 by enabling python-oracledb thick mode.
  if [ -n "${ORACLE_CLIENT_LIB_DIR:-}" ] || [ -n "${ORACLE_HOME:-}" ]; then
    return 0
  fi

  echo -e "${YELLOW}Oracle thick mode may be required for your server version.${RESET}"
  echo -e "To enable it, install Oracle Instant Client and set ORACLE_CLIENT_LIB_DIR (or ORACLE_HOME)."
  echo -e "Docs: https://python-oracledb.readthedocs.io/en/latest/user_guide/installation.html#installing-oracle-client-libraries"
  read -r -p "Path to Oracle Instant Client directory (leave blank to continue without thick mode): " oci_dir

  if [ -z "${oci_dir}" ]; then
    echo -e "${YELLOW}Continuing without thick mode. You may encounter DPY-3010 if the server is too old.${RESET}"
    return 0
  fi

  if [ ! -d "${oci_dir}" ]; then
    echo -e "${RED}${ERROR} Directory not found: ${oci_dir}${RESET}"
    return 0
  fi

  if [ ! -f "${oci_dir}/libclntsh.dylib" ] && [ ! -f "${oci_dir}/libclntsh.so" ] && [ ! -f "${oci_dir}/oci.dll" ]; then
    echo -e "${YELLOW}Warning: Could not find libclntsh in ${oci_dir}. Make sure this is the Instant Client directory.${RESET}"
  fi

  export ORACLE_CLIENT_LIB_DIR="${oci_dir}"

  # Optionally help dynamic linker find libs (usually not needed when lib_dir is passed, but safe)
  uname_s="$(uname -s 2>/dev/null || echo "")"
  case "${uname_s}" in
    Linux)
      export LD_LIBRARY_PATH="${ORACLE_CLIENT_LIB_DIR}:${LD_LIBRARY_PATH:-}"
      ;;
    Darwin)
      export DYLD_LIBRARY_PATH="${ORACLE_CLIENT_LIB_DIR}:${DYLD_LIBRARY_PATH:-}"
      ;;
    MINGW*|MSYS*|CYGWIN*)
      # Windows: python-oracledb uses PATH; advise but don't modify here.
      ;;
  esac

  echo -e "${GREEN}${CHECK} Using Oracle Instant Client at: ${ORACLE_CLIENT_LIB_DIR}${RESET}"

  # Offer to persist into .env (POSIX-compatible, no ${var,,})
  read -r -p "Persist ORACLE_CLIENT_LIB_DIR to .env for future runs? [y/N]: " persist
  case "$persist" in
    y|Y)
      if [ -f .env ]; then
        cp .env ".env.bak.$(date +%s)"
        grep -v '^ORACLE_CLIENT_LIB_DIR=' .env > .env.tmp || true
        mv .env.tmp .env
      fi
      escaped_val=$(printf '%s' "$ORACLE_CLIENT_LIB_DIR" | sed 's/"/\\"/g')
      echo "ORACLE_CLIENT_LIB_DIR=\"${escaped_val}\"" >> .env
      echo -e "${GREEN}${CHECK} Saved to .env${RESET}"
      ;;
    *)
      ;;
  esac
}

is_oracle_url() {
  # returns 0 if the db url looks like Oracle
  local url="${1:-}"
  [[ "$url" =~ ^oracle\+ ]] || [[ "$url" == oracle://* ]] || [[ "$url" == *":oracle"* ]]
}

# 📋 Menu
echo -e "Choose an option:"
echo -e "1) $SQL_EMOJI Generate SQL from a question"
echo -e "2) $DB_EMOJI Generate & Execute SQL on DB"
echo -e "3) $SERVER_EMOJI Run FastAPI Server"
echo -e "4) ❎ Exit"

read -r -p "Enter your choice [1-4]: " choice

case $choice in
  1)
    echo -e "${CYAN}Enter your natural language question:${RESET}"
    read -r question
    echo -e "${CYAN}Path to tables JSON (default: ${DEFAULT_TABLES}):${RESET}"
    read -r tables
    tables=${tables:-$DEFAULT_TABLES}

    echo -e "${CYAN}Model (default: ${DEFAULT_MODEL}):${RESET}"
    read -r model
    model=${model:-$DEFAULT_MODEL}

    dialect="$(choose_dialect false)"

    echo -e "${ROCKET} Running generate-sql..."
    "${RUNNER[@]}" src/cli.py generate-sql "$question" --tables "$tables" --model "$model" --dialect "$dialect"
    ;;

  2)
    echo -e "${CYAN}Enter your natural language question:${RESET}"
    read -r question
    echo -e "${CYAN}Path to tables JSON (default: ${DEFAULT_TABLES}):${RESET}"
    read -r tables
    tables=${tables:-$DEFAULT_TABLES}

    echo -e "${CYAN}Model (default: ${DEFAULT_MODEL}):${RESET}"
    read -r model
    model=${model:-$DEFAULT_MODEL}

    echo -e "${CYAN}Enter your DB URL (leave blank to build interactively):${RESET}
Examples:
  - PostgreSQL: postgresql+asyncpg://user:pass@host:5432/dbname
  - MySQL:      mysql+aiomysql://user:pass@host:3306/dbname
  - Oracle:     oracle+oracledb://user:pass@host:1521/?service_name=XE"
    if [ -n "$DEFAULT_DB_URL" ]; then
      echo -e "${CYAN}(default from \$DB_URL):${RESET} ${DEFAULT_DB_URL}"
    fi
    read -r db_url
    db_url=${db_url:-$DEFAULT_DB_URL}

    dialect="$(choose_dialect true)"

    # If Oracle (explicitly chosen or inferred from URL), ensure thick mode is configured when needed
    if [ "$dialect" = "oracle" ] || { [ "$dialect" = "auto" ] && is_oracle_url "$db_url"; }; then
      ensure_oracle_client
    fi

    # If user didn't provide a URL and dialect is oracle (or auto) -> build interactively
    if { [ -z "$db_url" ] && [ "$dialect" = "oracle" ]; } || { [ -z "$db_url" ] && [ "$dialect" = "auto" ]; }; then
      db_url="$(build_oracle_url)"
    else
      # Offer to rebuild a safer Oracle URL if they chose oracle or the URL looks oracle-like
      if [ "$dialect" = "oracle" ] || is_oracle_url "$db_url"; then
        echo -e "${CYAN}Would you like to rebuild the Oracle URL interactively (handles service vs SID and encodes special characters)? [y/N]${RESET}"
        read -r rebuild
        case "$rebuild" in
          y|Y) db_url="$(build_oracle_url)";;
          *) :;;
        esac
      fi
    fi

    if [ -z "$db_url" ]; then
      echo -e "${RED}${ERROR} DB URL is required.${RESET}"
      exit 1
    fi

    echo -e "${ROCKET} Running execute-sql..."
    if [ "$dialect" = "auto" ]; then
      "${RUNNER[@]}" src/cli.py execute-sql "$question" --tables "$tables" --model "$model" --db-url "$db_url"
    else
      "${RUNNER[@]}" src/cli.py execute-sql "$question" --tables "$tables" --model "$model" --db-url "$db_url" --dialect "$dialect"
    fi
    ;;

  3)
    echo -e "${CYAN}Host (default: ${DEFAULT_HOST}):${RESET}"
    read -r host
    host=${host:-$DEFAULT_HOST}

    echo -e "${CYAN}Port (default: ${DEFAULT_PORT}):${RESET}"
    read -r port
    port=${port:-$DEFAULT_PORT}

    echo -e "${ROCKET} Starting FastAPI server..."
    "${RUNNER[@]}" src/cli.py serve --host "$host" --port "$port"
    ;;

  4)
    echo -e "${GREEN}${CHECK} Exiting. Goodbye!${RESET}"
    exit 0
    ;;

  *)
    echo -e "${RED}${ERROR} Invalid choice!${RESET}"
    exit 1
    ;;
esac