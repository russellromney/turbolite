"""Override bdist_wheel so the wheel is tagged as platform-specific.

The package bundles a native .so/.dylib, so it must NOT be marked
as a pure-Python wheel (py3-none-any). This produces e.g.
turbolite-0.1.0-py3-none-macosx_11_0_arm64.whl
"""

from setuptools import setup
from wheel.bdist_wheel import bdist_wheel


class PlatformWheel(bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False

    def get_tag(self):
        _, _, plat = super().get_tag()
        return "py3", "none", plat


setup(cmdclass={"bdist_wheel": PlatformWheel})
