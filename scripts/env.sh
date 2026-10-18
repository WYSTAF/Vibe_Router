# Source before any cargo command in this Git-Bash shell:
#   source scripts/env.sh
# Adds cargo and — critically — the MSVC linker to PATH ahead of Git's coreutils
# `link.exe`, plus the Windows SDK LIB/INCLUDE the linker needs.
export PATH="$HOME/.cargo/bin:$PATH"
VS="/c/Program Files (x86)/Microsoft Visual Studio/2022/BuildTools"
MSVC="$VS/VC/Tools/MSVC/14.44.35207"
SDK="/c/Program Files (x86)/Windows Kits/10"
export PATH="$MSVC/bin/Hostx64/x64:$PATH"
export LIB="$MSVC/lib/x64;$SDK/Lib/10.0.26100.0/ucrt/x64;$SDK/Lib/10.0.26100.0/um/x64"
export INCLUDE="$MSVC/include;$SDK/Include/10.0.26100.0/ucrt;$SDK/Include/10.0.26100.0/um;$SDK/Include/10.0.26100.0/shared"
export RUST_MIN_STACK=16777216
