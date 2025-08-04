#!/bin/bash
# Add Google Chrome repository
wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list
# Update package list
apt-get update
# Install dependencies
apt-get install -y google-chrome-stable chromedriver libglib2.0-0 libnss3 libgconf-2-4 libfontconfig1 libx11-6 libxext6