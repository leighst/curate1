from setuptools import find_packages, setup

setup(
    name="curate1",
    packages=find_packages(exclude=["curate1_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
