description = "Demo Python scripts to automating MicroStrategy devOps processes"
from distutils.core import setup
from distutils.command.install import INSTALL_SCHEMES
from pathlib import Path
this_directory = Path(__file__).parent

for scheme in list(INSTALL_SCHEMES.values()):
    scheme['data'] = scheme['purelib']

long_description = (this_directory / "README.md").read_text()
setup(name='MSTR_Robotics_magerdaniel',
      version= "0.3.02",
      description="MicroStrateg(P)ython",
      author="Daniel Mager",
      author_email='"danielmager@gmx.de"',
      url="https://github.com/magerdaniel",
      long_description=long_description,
      long_description_content_type='text/markdown',
      packages=['mstr_robotics'],
      install_requires=[
        "mstrio-py",  # MicroStrategy Python Library
        "langchain",  # Framework for developing LLM-powered applications
        "openai",  # OpenAI Python client
        "flashtext",  # Library for keyword extraction and replacement
        # itertools is a standard Python library and does not need to be installed
        ],
      package_data={'mstr_robotics': ['jup_schema_monitor.ipynb']},
)
