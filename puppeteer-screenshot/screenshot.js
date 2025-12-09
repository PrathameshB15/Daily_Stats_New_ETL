// puppeteer-screenshot/screenshot.js
const puppeteer = require('puppeteer');
const path = require('path');

(async () => {
  const inputPath = process.argv[2]; // HTML file path
  const outputPath = process.argv[3]; // PNG path

  if (!inputPath || !outputPath) {
    console.error("Usage: node screenshot.js <input.html> <output.png>");
    process.exit(1);
  }

  const browser = await puppeteer.launch();
  const page = await browser.newPage();

  const fileUrl = 'file://' + path.resolve(inputPath);
  await page.goto(fileUrl, { waitUntil: 'networkidle0' });

  await page.screenshot({
    path: outputPath,
    fullPage: true,
    type: 'png'
  });

  await browser.close();
})();
