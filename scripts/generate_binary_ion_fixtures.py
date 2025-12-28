import os
import subprocess
import sys
from pathlib import Path

TEXT_SUFFIX = "_text.ion"
EXTRA_TEXT_FIXTURES = {
    "conflicts.ion": "conflicts_binary.ion",
    "nested.ion": "nested_binary.ion",
    "sample.ion": "sample_binary.ion",
    "scalars.ion": "scalars_binary.ion",
}


def resolve_triplet() -> str:
    os_name = os.uname().sysname.lower()
    if os_name == "darwin":
        os_name = "osx"
    arch = os.uname().machine
    return os.environ.get("VCPKG_TARGET_TRIPLET", f"{arch}-{os_name}")


def find_vcpkg_paths() -> tuple[Path, Path] | None:
    triplet = resolve_triplet()
    local_prefix = Path("vcpkg_installed") / triplet
    vcpkg_root = os.environ.get("VCPKG_ROOT", "")
    vcpkg_prefix = Path(vcpkg_root) / "installed" / triplet if vcpkg_root else None

    for prefix in (local_prefix, vcpkg_prefix):
        if prefix and prefix.is_dir():
            include_dir = prefix / "include"
            lib_dir = prefix / "lib"
            if include_dir.is_dir() and lib_dir.is_dir():
                return include_dir, lib_dir
    return None


def build_tool(tool_path: Path) -> bool:
    paths = find_vcpkg_paths()
    if not paths:
        return False
    include_dir, lib_dir = paths
    cxx = os.environ.get("CXX", "c++")
    source = Path("scripts/ion_text_to_binary.cpp")
    if not source.exists():
        raise FileNotFoundError(source)
    tool_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        cxx,
        "-std=c++17",
        "-O2",
        f"-I{include_dir}",
        str(source),
        str(lib_dir / "libionc_static.a"),
        str(lib_dir / "libdecNumber_static.a"),
        "-o",
        str(tool_path),
    ]
    subprocess.run(cmd, check=True)
    return tool_path.exists()


def resolve_tool() -> Path:
    env_tool = os.environ.get("ION_TEXT_TO_BINARY")
    if env_tool:
        return Path(env_tool)
    return Path("build/ion_text_to_binary")


def ensure_tool() -> Path:
    tool = resolve_tool()
    if tool.exists() and os.access(tool, os.X_OK):
        return tool
    if build_tool(tool) and os.access(tool, os.X_OK):
        return tool
    raise RuntimeError(
        "ion text-to-binary tool not found. Set ION_TEXT_TO_BINARY to a built binary, "
        "or install ion-c via vcpkg so this script can build scripts/ion_text_to_binary.cpp."
    )


def convert_fixture(tool: Path, base: Path, text_name: str, binary_name: str) -> None:
    text_path = base / text_name
    if not text_path.exists():
        raise FileNotFoundError(text_path)
    binary_path = base / binary_name
    subprocess.run([str(tool), str(text_path), str(binary_path)], check=True)


def main() -> int:
    tool = ensure_tool()
    base = Path("test/ion")
    fixtures: list[tuple[str, str]] = []
    for text_path in sorted(base.glob(f"*{TEXT_SUFFIX}")):
        text_name = text_path.name
        binary_name = text_name.replace(TEXT_SUFFIX, "_binary.ion")
        fixtures.append((text_name, binary_name))
    for text_name, binary_name in EXTRA_TEXT_FIXTURES.items():
        fixtures.append((text_name, binary_name))
    for text_name, binary_name in fixtures:
        convert_fixture(tool, base, text_name, binary_name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
