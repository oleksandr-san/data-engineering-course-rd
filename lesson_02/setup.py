from setuptools import find_packages, setup

setup(
    name="lesson_02",
    version="0.1.0",
    packages=find_packages(include=["job1", "job1.*", "job2", "job2.*"]),
)
