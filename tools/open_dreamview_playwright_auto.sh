#!/usr/bin/env bash
set -euo pipefail

URL="${1:-http://localhost:8888}"
CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
PWCLI="${PWCLI:-$CODEX_HOME/skills/playwright/scripts/playwright_cli.sh}"
SESSION="${DREAMVIEW_PLAYWRIGHT_SESSION:-dreamview_auto}"
WIDTH="${DREAMVIEW_CHROME_WIDTH:-1280}"
HEIGHT="${DREAMVIEW_CHROME_HEIGHT:-720}"
AUTO_TIMEOUT_SEC="${DREAMVIEW_AUTO_ENTER_TIMEOUT_SEC:-30}"

if [ ! -x "${PWCLI}" ]; then
  echo "playwright wrapper not found: ${PWCLI}" >&2
  exit 2
fi

"${PWCLI}" --session "${SESSION}" open "${URL}" --headed >/tmp/dreamview_playwright_open.log 2>&1 || true
"${PWCLI}" --session "${SESSION}" resize "${WIDTH}" "${HEIGHT}" >/tmp/dreamview_playwright_resize.log 2>&1 || true

deadline=$((SECONDS + AUTO_TIMEOUT_SEC))
while [ "${SECONDS}" -lt "${deadline}" ]; do
  "${PWCLI}" --session "${SESSION}" run-code "$(cat <<'JS'
async (page) => {
async function clickVisible(locator) {
  const count = await locator.count().catch(() => 0);
  if (!count) return false;
  const item = locator.first();
  const visible = await item.isVisible().catch(() => false);
  if (!visible) return false;
  await item.click({ timeout: 1500 }).catch(() => {});
  return true;
}

await page.waitForLoadState('domcontentloaded').catch(() => {});

const welcomeText = page.getByText('Accept the User Agreement and Privacy Policy');
if (await welcomeText.count().catch(() => 0)) {
  const checkbox = page.getByRole('checkbox').first();
  const checked = await checkbox.isChecked().catch(() => false);
  if (!checked) {
    await checkbox.check({ timeout: 1500 }).catch(async () => {
      await welcomeText.click({ timeout: 1500 }).catch(() => {});
    });
  }
  const enter = page.getByRole('button', { name: /Enter this mode/i }).first();
  const enabled = await enter.isEnabled().catch(() => false);
  if (enabled) {
    await enter.click({ timeout: 1500 }).catch(() => {});
  }
}

await page.waitForTimeout(800);
await clickVisible(page.getByRole('button', { name: /Skip/i }));

const ready =
  (await page.getByText('Vehicle Visualization').count().catch(() => 0)) > 0 ||
  (await page.getByText('Mode Settings').count().catch(() => 0)) > 0;
if (!ready) throw new Error('Dreamview main UI not ready yet');
}
JS
)" >/tmp/dreamview_playwright_auto.log 2>&1 || true
  if ! grep -q "### Error" /tmp/dreamview_playwright_auto.log; then
    exit 0
  fi
  sleep 1
done

cat /tmp/dreamview_playwright_auto.log >&2 || true
exit 1
