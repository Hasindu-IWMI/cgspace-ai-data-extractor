#!/bin/bash
# Add Google Chrome repository key and source
wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-chrome-archive-keyring.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-archive-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" | tee /etc/apt/sources.list.d/google-chrome.list
# Update package list
apt-get update
# Install Chrome, ChromeDriver, and required libraries
apt-get install -y google-chrome-stable chromedriver libglib2.0-0 libnss3 libgconf-2-4 libfontconfig1 libx11-6 libxext6
