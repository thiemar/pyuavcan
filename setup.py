from setuptools import setup, find_packages

setup(
    name="pyuavcan",
    version="1.0.0dev1",
    description="",
    url="https://github.com/thiemar/pyuavcan",
    author="",
    author_email="",
    license="MIT",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.7",
    ],
    keywords="",
    packages=["uavcan"],
    install_requires=["tornado>=4.1"],
)
