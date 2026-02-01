#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Stagecrew Installation Script
# ============================================================================
# A disk usage management tool for shared HPC staging filesystems.
# Enforces removal-by-default policy with configurable expiration periods.
#
# Repository: https://github.com/nrminor/stagecrew
# License: MIT
# ============================================================================

REPO="nrminor/stagecrew"
BINARY_NAME="stagecrew"
VERSION="0.1.0"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Helper functions
info() { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }
step() { echo -e "${BLUE}[STEP]${NC} $1"; }

# Check if we're in a conda/mamba/pixi environment
in_conda_env() {
	[[ -n "${CONDA_PREFIX:-}" ]]
}

# Determine installation directory based on environment
determine_install_dir() {
	if [[ -n "${CONDA_PREFIX:-}" ]]; then
		# In conda/mamba/pixi environment - install to environment bin
		echo "${CONDA_PREFIX}/bin"
	else
		# Global install - use XDG-compliant user bin
		echo "${HOME}/.local/bin"
	fi
}

# Get cargo install root (strips /bin suffix)
get_cargo_install_root() {
	local install_dir="$1"
	echo "${install_dir%/bin}"
}

# Get a friendly name for the current environment type
get_environment_name() {
	if [[ -n "${PIXI_PROJECT_ROOT:-}" ]]; then
		echo "pixi"
	elif [[ -n "${CONDA_PREFIX:-}" ]]; then
		# Could be conda or mamba, but we treat them the same
		if [[ -n "${CONDA_DEFAULT_ENV:-}" ]]; then
			echo "conda (${CONDA_DEFAULT_ENV})"
		else
			echo "conda"
		fi
	else
		echo "none"
	fi
}

# Detect OS and architecture
detect_platform() {
	local os arch

	case "$(uname -s)" in
	Linux) os="linux" ;;
	Darwin) os="darwin" ;;
	MINGW* | MSYS* | CYGWIN*) os="windows" ;;
	*)
		error "Unsupported OS: $(uname -s)"
		exit 1
		;;
	esac

	case "$(uname -m)" in
	x86_64 | amd64) arch="x86_64" ;;
	aarch64 | arm64) arch="aarch64" ;;
	*)
		error "Unsupported architecture: $(uname -m)"
		exit 1
		;;
	esac

	# Map to Rust target triple
	case "${os}-${arch}" in
	linux-x86_64) echo "x86_64-unknown-linux-musl" ;;
	linux-aarch64) echo "aarch64-unknown-linux-musl" ;;
	darwin-x86_64) echo "x86_64-apple-darwin" ;;
	darwin-aarch64) echo "aarch64-apple-darwin" ;;
	windows-x86_64) echo "x86_64-pc-windows-msvc" ;;
	windows-aarch64) echo "aarch64-pc-windows-msvc" ;;
	*)
		error "Unsupported platform: ${os}-${arch}"
		exit 1
		;;
	esac
}

# Get latest release tag from GitHub
get_latest_release() {
	local url="https://api.github.com/repos/${REPO}/releases/latest"
	if command -v curl &>/dev/null; then
		curl -fsSL "$url" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/'
	elif command -v wget &>/dev/null; then
		wget -qO- "$url" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/'
	else
		error "Neither curl nor wget found. Please install one of them."
		exit 1
	fi
}

# Download and install binary
install_binary() {
	local version="$1"
	local platform="$2"
	local install_dir="$3"
	local archive_ext="tar.gz"

	if [[ "$platform" == *"windows"* ]]; then
		archive_ext="zip"
	fi

	local download_url="https://github.com/${REPO}/releases/download/${version}/${BINARY_NAME}-${platform}.${archive_ext}"
	local temp_dir
	temp_dir=$(mktemp -d) || {
		error "Failed to create temp directory"
		return 1
	}

	step "Downloading ${BINARY_NAME} ${version} for ${platform}..."

	cd "$temp_dir" || {
		error "Failed to enter temp directory"
		rm -rf "$temp_dir"
		return 1
	}
	if command -v curl &>/dev/null; then
		curl -fsSL -o "archive.${archive_ext}" "$download_url" || return 1
	else
		wget -q -O "archive.${archive_ext}" "$download_url" || return 1
	fi

	step "Extracting binary..."
	if [[ "$archive_ext" == "zip" ]]; then
		unzip -q "archive.${archive_ext}" || {
			error "Failed to extract zip archive"
			cd - >/dev/null || true
			rm -rf "$temp_dir"
			return 1
		}
	else
		tar -xzf "archive.${archive_ext}" || {
			error "Failed to extract tar archive"
			cd - >/dev/null || true
			rm -rf "$temp_dir"
			return 1
		}
	fi

	step "Installing to ${install_dir}..."
	mkdir -p "$install_dir"

	if [[ -f "${BINARY_NAME}" ]]; then
		chmod +x "${BINARY_NAME}"
		mv "${BINARY_NAME}" "${install_dir}/"
	elif [[ -f "${BINARY_NAME}.exe" ]]; then
		mv "${BINARY_NAME}.exe" "${install_dir}/"
	else
		error "Binary not found in archive"
		cd - >/dev/null || true
		rm -rf "$temp_dir"
		return 1
	fi

	cd - >/dev/null || true
	# Safety check: only remove if temp_dir is set and looks like a temp directory
	if [[ -n "$temp_dir" && "$temp_dir" == /tmp/* || "$temp_dir" == /var/folders/* ]]; then
		rm -rf "$temp_dir"
	fi

	info "Successfully installed ${BINARY_NAME} to ${install_dir}"
	return 0
}

# Provide helpful error when cargo is missing in conda environment
fail_no_cargo_in_env() {
	local env_type="$1"

	error "Cargo not found in active environment"
	echo ""

	case "$env_type" in
	pixi*)
		info "Add Rust to your pixi.toml:"
		echo "  pixi add rust"
		echo ""
		echo "Then re-run this installer"
		;;
	conda*)
		info "Install Rust in your conda/mamba environment:"
		echo "  conda install -c conda-forge rust"
		echo "  # or"
		echo "  mamba install rust"
		echo ""
		echo "Then re-run this installer"
		;;
	*)
		info "Install Rust in your environment or globally:"
		echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
		;;
	esac

	exit 1
}

# Build from source
build_from_source() {
	local install_dir="$1"
	local install_root
	install_root=$(get_cargo_install_root "$install_dir")

	step "Building from source..."

	# Check for Rust
	if ! command -v cargo &>/dev/null; then
		if in_conda_env; then
			# In conda environment - fail with helpful message
			local env_type
			env_type=$(get_environment_name)
			fail_no_cargo_in_env "$env_type"
		else
			# Global install - auto-install rustup
			warn "Rust not found. Installing Rust toolchain..."
			curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
			export PATH="$HOME/.cargo/bin:$PATH"
		fi
	fi

	info "Installing ${BINARY_NAME} from GitHub repository..."
	info "Using cargo: $(command -v cargo)"
	info "Install root: ${install_root}"

	# Use cargo install with --root to control installation directory
	if cargo install --git "https://github.com/${REPO}.git" --root "${install_root}" --force; then
		info "Successfully built and installed ${BINARY_NAME}"
	else
		error "Failed to install from source"
		exit 1
	fi
}

# Show help message
show_help() {
	echo -e "${BOLD}Stagecrew Installer${NC} - Version ${VERSION}"
	echo ""
	echo -e "${BOLD}DESCRIPTION${NC}"
	echo "    A disk usage management tool for shared HPC staging filesystems."
	echo "    Enforces removal-by-default policy with configurable expiration periods."
	echo ""
	echo -e "${BOLD}SYNOPSIS${NC}"
	echo "    $0 [OPTIONS]"
	echo ""
	echo -e "${BOLD}OPTIONS${NC}"
	echo "    -h, --help          Show this help message and exit"
	echo "    -v, --version       Show version information and exit"
	echo ""
	echo -e "${BOLD}INSTALLATION BEHAVIOR${NC}"
	echo "    The installer automatically detects your environment and installs to the"
	echo "    appropriate location:"
	echo ""
	echo -e "    ${BOLD}Conda/Mamba/Pixi Environments:${NC}"
	echo "        When a conda-compatible environment is active (\$CONDA_PREFIX is set),"
	echo "        the binary will be installed to:"
	echo "            \$CONDA_PREFIX/bin"
	echo ""
	echo "        This ensures the tool is isolated within your project environment and"
	echo "        automatically available in your PATH when the environment is activated."
	echo ""
	echo -e "    ${BOLD}Global Installation:${NC}"
	echo "        When no conda-compatible environment is detected, the binary will be"
	echo "        installed to:"
	echo "            \$HOME/.local/bin"
	echo ""
	echo "        You may need to add this directory to your PATH if it's not already."
	echo ""
	echo -e "${BOLD}INSTALLATION METHODS${NC}"
	echo -e "    1. ${BOLD}Pre-built binary${NC} (preferred)"
	echo "       Downloads the latest release binary for your platform from GitHub."
	echo ""
	echo -e "    2. ${BOLD}Build from source${NC} (fallback)"
	echo "       If no pre-built binary is available, builds from source using cargo."
	echo "       - In conda environments: Requires 'rust' package to be installed"
	echo "       - Global installs: Automatically installs rustup if needed"
	echo ""
	echo -e "${BOLD}REQUIREMENTS${NC}"
	echo "    Runtime:"
	echo "        - SQLite (bundled, no external dependency)"
	echo "        - Shared filesystem (e.g., CephFS) for multi-user scenarios"
	echo ""
	echo "    Build from source (if needed):"
	echo "        - Rust toolchain (cargo)"
	echo "        - Internet connection"
	echo ""
	echo -e "${BOLD}POST-INSTALLATION${NC}"
	echo "    After installation, you'll need to:"
	echo "    1. Run 'stagecrew init' to create config and database"
	echo "    2. Edit ~/.config/stagecrew/config.toml to add tracked paths"
	echo "    3. Run 'stagecrew scan' to perform initial scan"
	echo "    4. Use 'stagecrew' (TUI) or 'stagecrew daemon' for ongoing management"
	echo ""
	echo "    See the full documentation at: https://github.com/${REPO}"
	echo ""
	echo -e "${BOLD}EXAMPLES${NC}"
	echo "    # Install in current conda environment"
	echo "    conda activate myenv"
	echo "    curl -fsSL https://raw.githubusercontent.com/${REPO}/main/INSTALL.sh | bash"
	echo ""
	echo "    # Install in pixi project"
	echo "    cd my-pixi-project"
	echo "    pixi shell"
	echo "    curl -fsSL https://raw.githubusercontent.com/${REPO}/main/INSTALL.sh | bash"
	echo ""
	echo "    # Global installation"
	echo "    curl -fsSL https://raw.githubusercontent.com/${REPO}/main/INSTALL.sh | bash"
	echo ""
	echo -e "${BOLD}AUTHOR${NC}"
	echo "    Nicholas R. Minor <nrminor@wisc.edu>"
	echo ""
	echo -e "${BOLD}LICENSE${NC}"
	echo "    MIT License - See https://github.com/${REPO}/blob/main/LICENSE"
	echo ""
	echo -e "${BOLD}REPORTING BUGS${NC}"
	echo "    Report bugs at: https://github.com/${REPO}/issues"
	echo ""
}

# Show version information
show_version() {
	cat <<EOF
${BINARY_NAME} installer version ${VERSION}
Repository: https://github.com/${REPO}
License: MIT
EOF
}

# Parse command line arguments
parse_args() {
	while [[ $# -gt 0 ]]; do
		case $1 in
		-h | --help)
			show_help
			exit 0
			;;
		-v | --version)
			show_version
			exit 0
			;;
		*)
			error "Unknown option: $1"
			echo "Use --help for usage information"
			exit 1
			;;
		esac
	done
}

# Verify installation and provide feedback
verify_installation() {
	local install_dir="$1"

	echo ""
	echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

	if in_conda_env; then
		# In conda environment - PATH is automatically managed
		local env_type
		env_type=$(get_environment_name)
		info "Installed to ${env_type} environment: ${install_dir}"
		info "Binary available as: ${BINARY_NAME}"
	else
		# Global install - check if in PATH
		info "Installed globally: ${install_dir}"
		if [[ ":$PATH:" != *":${install_dir}:"* ]]; then
			warn "${install_dir} is not in your PATH"
			info "Add to your shell configuration (~/.bashrc, ~/.zshrc, etc.):"
			echo "  export PATH=\"${install_dir}:\$PATH\""
			echo ""
		fi
	fi

	# Try to verify binary is accessible
	if command -v "${BINARY_NAME}" &>/dev/null; then
		echo -e "${GREEN}${BOLD}✓ Installation successful!${NC}"
		info "Run '${BINARY_NAME} --help' to get started"
	else
		if in_conda_env; then
			info "Binary installed: ${install_dir}/${BINARY_NAME}"
		else
			warn "Binary installed but not found in PATH"
			info "Run directly: ${install_dir}/${BINARY_NAME}"
		fi
	fi

	echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

# Show post-installation instructions
show_next_steps() {
	echo ""
	echo -e "${BOLD}Next Steps:${NC}"
	echo ""
	echo -e "${BOLD}1. Initialize Stagecrew${NC}"
	echo "   stagecrew init"
	echo ""
	echo -e "${BOLD}2. Configure Tracked Paths${NC}"
	echo "   Edit ~/.config/stagecrew/config.toml and add paths to monitor:"
	echo ""
	echo "   tracked_paths = [\"/scratch/myproject\", \"/staging/data\"]"
	echo "   expiration_days = 90"
	echo "   warning_days = 14"
	echo ""
	echo -e "${BOLD}3. Run Initial Scan${NC}"
	echo "   stagecrew scan"
	echo ""
	echo -e "${BOLD}4. Launch TUI or Daemon${NC}"
	echo "   stagecrew        # Interactive TUI"
	echo "   stagecrew daemon # Background daemon"
	echo ""
	echo -e "${BOLD}For More Information:${NC}"
	echo "   Documentation: https://github.com/${REPO}"
	echo "   Issues: https://github.com/${REPO}/issues"
	echo ""
}

# Main installation logic
main() {
	parse_args "$@"

	# Print banner
	echo ""
	echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	echo -e "${BOLD}  Stagecrew Installer${NC}"
	echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	echo ""

	# Detect environment and determine install location
	local install_dir
	install_dir=$(determine_install_dir)

	if in_conda_env; then
		local env_type
		env_type=$(get_environment_name)
		info "Detected environment: ${env_type}"
		info "Environment prefix: ${CONDA_PREFIX}"
	else
		info "No conda environment detected (global installation)"
	fi

	info "Install directory: ${install_dir}"

	# Detect platform
	local platform
	platform=$(detect_platform)
	info "Detected platform: ${platform}"

	echo ""

	# Try to download pre-built binary
	local version
	version=$(get_latest_release)

	if [[ -n "$version" ]]; then
		info "Latest release: ${version}"
		if install_binary "$version" "$platform" "$install_dir"; then
			verify_installation "$install_dir"
			show_next_steps
			return 0
		fi
		warn "Failed to download pre-built binary"
		echo ""
	else
		warn "Could not determine latest release"
		echo ""
	fi

	# Fall back to source build
	warn "Falling back to source build..."
	build_from_source "$install_dir"
	verify_installation "$install_dir"
	show_next_steps
}

main "$@"
