from setuptools import setup, find_packages
from pathlib import Path

# Read the long description from README.md
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="MSTR_Robotics_magerdaniel",
    version="0.3.06",
    description="MicroStrateg(P)ython",
    author="Daniel Mager",
    author_email="danielmager@gmx.de",
    url="https://github.com/magerdaniel",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),  # Automatically find all packages
    install_requires=[
        "mstrio-py==11.4.6.101",  # Specify the exact or compatible version
        "langchain",
        "openai",
        "flashtext>=2.7",  # Add a minimum version if applicable
    ],
    package_data={"mstr_robotics": ["jup_schema_monitor.ipynb"]},  # Include additional files
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",  # Specify the required Python version
)
