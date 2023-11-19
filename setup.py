#[tool.poetry]
description = "Demo Python scripts to automating MicroStrategy devOps processes"

#from distutils.core import setup
from setuptools import setup, find_packages
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()
setup(name='MSTR_Robotics_magerdaniel',
      version= "0.2.3",
      description="MSTR_Robotics The Schema Monitor",
      author="Daniel Mager",
      author_email='"danielmager@gmx.de"',
      url="https://github.com/magerdaniel",
      long_description=long_description,
      packages=find_packages(),
      include_package_data=True,
      long_description_content_type='text/markdown',


)

