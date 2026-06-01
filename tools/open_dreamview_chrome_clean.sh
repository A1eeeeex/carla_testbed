#!/usr/bin/env bash
set -euo pipefail

URL="${1:-http://localhost:8888}"
PROFILE_DIR="${DREAMVIEW_CHROME_PROFILE:-/tmp/carla_testbed_dreamview_chrome_profile}"
RESET_PROFILE="${DREAMVIEW_CHROME_RESET_PROFILE:-0}"
WIDTH="${DREAMVIEW_CHROME_WIDTH:-1280}"
HEIGHT="${DREAMVIEW_CHROME_HEIGHT:-720}"
POS_X="${DREAMVIEW_CHROME_POS_X:-0}"
POS_Y="${DREAMVIEW_CHROME_POS_Y:-0}"

if [ "${RESET_PROFILE}" = "1" ]; then
  rm -rf "${PROFILE_DIR}"
fi
mkdir -p "${PROFILE_DIR}"

exec /usr/bin/google-chrome \
  --user-data-dir="${PROFILE_DIR}" \
  --no-first-run \
  --no-default-browser-check \
  --disable-extensions \
  --disable-session-crashed-bubble \
  --disable-infobars \
  --force-device-scale-factor=1 \
  --new-window \
  --app="${URL}" \
  --window-size="${WIDTH},${HEIGHT}" \
  --window-position="${POS_X},${POS_Y}"
