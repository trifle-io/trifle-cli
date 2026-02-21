#!/bin/sh
set -e

REPO="trifle-io/trifle-cli"
BINARY="trifle"
INSTALL_DIR="/usr/local/bin"

# Detect OS
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$OS" in
  darwin) OS="darwin" ;;
  linux)  OS="linux" ;;
  *)
    echo "Error: Unsupported operating system: $OS"
    echo "Trifle CLI supports macOS and Linux."
    exit 1
    ;;
esac

# Detect architecture
ARCH=$(uname -m)
case "$ARCH" in
  x86_64|amd64)  ARCH="amd64" ;;
  arm64|aarch64) ARCH="arm64" ;;
  *)
    echo "Error: Unsupported architecture: $ARCH"
    exit 1
    ;;
esac

# Get latest version from GitHub
echo "Fetching latest release..."
VERSION=$(curl -sSf "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"v([^"]+)".*/\1/')

if [ -z "$VERSION" ]; then
  echo "Error: Could not determine latest version."
  exit 1
fi

echo "Installing Trifle CLI v${VERSION} (${OS}/${ARCH})..."

# Download
URL="https://github.com/${REPO}/releases/download/v${VERSION}/${BINARY}_${VERSION}_${OS}_${ARCH}.tar.gz"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

curl -sSfL "$URL" -o "${TMP}/trifle.tar.gz"
tar -xzf "${TMP}/trifle.tar.gz" -C "$TMP"

# Install
if [ -w "$INSTALL_DIR" ]; then
  mv "${TMP}/${BINARY}" "${INSTALL_DIR}/${BINARY}"
else
  echo "Installing to ${INSTALL_DIR} (requires sudo)..."
  sudo mv "${TMP}/${BINARY}" "${INSTALL_DIR}/${BINARY}"
fi

chmod +x "${INSTALL_DIR}/${BINARY}"

echo "Trifle CLI v${VERSION} installed to ${INSTALL_DIR}/${BINARY}"
echo ""
echo "Run 'trifle version' to verify."
