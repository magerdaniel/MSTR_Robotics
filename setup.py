#[tool.poetry]
description = "Demo Python scripts to automating MicroStrategy devOps processes"

from distutils.core import setup
#from setuptools import setup
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()
setup(name='MSTR_Robotics_magerdaniel',
      version= "0.1.0",
      description="MicroStrateg(P)ython",
      author="Daniel Mager",
      author_email='"danielmager@gmx.de"',
      url="https://github.com/magerdaniel",
      long_description=long_description,
      long_description_content_type='text/markdown',
      packages=['mstr_robotics'],

)

