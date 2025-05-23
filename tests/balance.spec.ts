import { test } from '@playwright/test';

const simulate_connection = async ({ page }) => {
  page.on('console', msg => console.log(msg.text()));
  page.on("pageerror", err => {
    console.log(err.message)
  })
  await page.goto('http://127.0.0.1:9000/publix/0zZcLtCCaKA');
  await page.waitForURL('**/endPage.html');
};

test('Study Ends', simulate_connection);
