#[tool.poetry]
description = "Demo Python scripts to automating MicroStrategy devOps processes"

from distutils.core import setup
setup(name='MSTR_Robotics_magerdaniel',
      version= "0.0.37",
      description="MicroStrateg(P)ython",
      author="Daniel Mager",
      author_email='"danielmager@gmx.de"',
      url="https://github.com/magerdaniel",
      packages=['mstr_robotics'],
      exclude = ["hello_beer.py"]
     # package_dir={
     #     'package2': 'package1',
     #     'package3': 'package1',
     # },
)