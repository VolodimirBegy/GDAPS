from setuptools import find_packages
from setuptools import setup


packages = find_packages()

_install_requires = [
    "simpy==3.0.11"
    "numpy",
]

_parameters = {
    "install_requires": _install_requires,
    "license": "BSD",
    "name": "GDAPS",
    "packages": packages,
    "platform": "any",
    "url": "https://github.com/VolodimirBegy/GDAPS/",
    "version": "0.0.1"
}

setup(**_parameters)
