#!/usr/bin/env bash
# Run Personal Diary Server directly on your Android Phone (via Termux) or Laptop
echo "📱 Starting Personal Diary Phone Server on http://localhost:3000 ..."
if ! command -v node >/dev/null 2>&1; then
  echo "Installing Node.js..."
  pkg install -y nodejs 2>/dev/null || sudo apt-get install -y nodejs
fi
node server.mjs
