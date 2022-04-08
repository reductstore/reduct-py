# Python implementation of Reduct Storage HTTP API
# (c) 2022 Alexey Timin

""" Setup script
"""
from pathlib import Path

from setuptools import setup, find_packages

PACKAGE_NAME = "reduct-py"
MAJOR_VERSION = 0
MINOR_VERSION = 1
PATCH_VERSION = 0

HERE = Path(__file__).parent.resolve()


def update_package_version(path: Path, version: str):
    """Overwrite/create __init__.py file and fill __version__"""
    with open(path / "VERSION", "w") as version_file:
        version_file.write(f"{version}\n")


def build_version():
    """Build dynamic version and update version in package"""
    version = f"{MAJOR_VERSION}.{MINOR_VERSION}.{PATCH_VERSION}"

    update_package_version(HERE / "pkg" / "reduct", version=version)

    return version


def get_long_description(base_path: Path):
    """Get long package description"""
    return (base_path / "README.md").read_text(encoding="utf-8")


setup(
    name=PACKAGE_NAME,
    version=build_version(),
    description="Reduct Storage Client SDK for Python",
    long_description=get_long_description(HERE),
    long_description_content_type="text/markdown",
    url="https://github.com/reduct-storage/reduct-py",
    author="Alexey Timin, Ciaran Moyne",
    author_email="atimin@gmai.com",
    package_dir={"": "pkg"},
    package_data={"": ["VERSION"]},
    packages=find_packages(where="pkg"),
    python_requires=">=3.8",
    install_requires=["aiohttp~=3.8", "pydantic~=1.9"],
    extras_require={
        "test": ["pytest~=6.2", "pytest-asyncio~=0.18"],
        "lint": [
            "pylint~=2.9",
        ],
        "format": ["black~=22.3"],
        "docs": [
            "mkdocs~=1.3",
            "mkdocs-material~=8.2",
            "plantuml-markdown~=3.5",
            "mkdocs-same-dir~=0.1",
            "mkdocstrings[python]~=0.18",
        ],
    },
)
