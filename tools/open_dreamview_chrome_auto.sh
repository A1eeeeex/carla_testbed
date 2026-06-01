#!/usr/bin/env bash
set -euo pipefail

URL="${1:-http://localhost:8888}"
PROFILE_DIR="${DREAMVIEW_CHROME_PROFILE:-/tmp/carla_testbed_dreamview_chrome_profile}"
RESET_PROFILE="${DREAMVIEW_CHROME_RESET_PROFILE:-1}"
WIDTH="${DREAMVIEW_CHROME_WIDTH:-1280}"
HEIGHT="${DREAMVIEW_CHROME_HEIGHT:-720}"
POS_X="${DREAMVIEW_CHROME_POS_X:-0}"
POS_Y="${DREAMVIEW_CHROME_POS_Y:-0}"
REMOTE_PORT="${DREAMVIEW_CHROME_REMOTE_DEBUGGING_PORT:-9222}"
AUTO_TIMEOUT_SEC="${DREAMVIEW_AUTO_ENTER_TIMEOUT_SEC:-45}"
LOG="${DREAMVIEW_CHROME_AUTO_LOG:-/tmp/dreamview_chrome_auto.log}"

if [ "${RESET_PROFILE}" = "1" ]; then
  rm -rf "${PROFILE_DIR}"
fi
mkdir -p "${PROFILE_DIR}"

if ! curl -fsS "http://127.0.0.1:${REMOTE_PORT}/json/version" >/dev/null 2>&1; then
  nohup /usr/bin/google-chrome \
    --user-data-dir="${PROFILE_DIR}" \
    --remote-debugging-address=127.0.0.1 \
    --remote-debugging-port="${REMOTE_PORT}" \
    --no-first-run \
    --no-default-browser-check \
    --disable-extensions \
    --disable-session-crashed-bubble \
    --disable-infobars \
    --force-device-scale-factor=1 \
    --new-window \
    --window-size="${WIDTH},${HEIGHT}" \
    --window-position="${POS_X},${POS_Y}" \
    "${URL}" \
    >"${LOG}.chrome" 2>&1 &
fi

deadline=$((SECONDS + AUTO_TIMEOUT_SEC))
while [ "${SECONDS}" -lt "${deadline}" ]; do
  curl -fsS "http://127.0.0.1:${REMOTE_PORT}/json/version" >/dev/null 2>&1 && break
  sleep 1
done

if ! curl -fsS "http://127.0.0.1:${REMOTE_PORT}/json/version" >/dev/null 2>&1; then
  echo "Chrome remote debugging did not become ready on ${REMOTE_PORT}" | tee "${LOG}"
  exit 1
fi

set +e
python3 /home/ubuntu/carla_testbed/tools/dreamview_chrome_cdp_auto.py \
  --url "${URL}" \
  --port "${REMOTE_PORT}" \
  --timeout-sec "${AUTO_TIMEOUT_SEC}" \
  >"/tmp/dreamview_chrome_auto_node.log" 2>&1
node_rc=$?
set -e
if [ "${node_rc}" -ne 0 ]; then
  cat /tmp/dreamview_chrome_auto_node.log >"${LOG}"
  exit "${node_rc}"
fi

cat /tmp/dreamview_chrome_auto_node.log >"${LOG}"
exit 0
