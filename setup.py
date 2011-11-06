from setuptools import setup, find_packages
import sys, os


if __name__ == '__main__':
    package = 'lamegame_tasking'
    version = '0.0'
    
    setup(name=package,
          version=version,
          description="lgTask provides a distributed, self organizing task processing architecture based around MongoDB",
          long_description="""\
    """,
          classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
          keywords='',
          author='Walt Woods',
          author_email='woodswalben@gmail.com',
          url='http://www.lamegameproductions.com',
          license='MIT',
          packages=find_packages(exclude=[ '*.test', '*.test.*' ]),
          package_data = {
              # Resources to include, e.g.: '': ['static/*','templates/*']
          },
          data_files = [
              # Loose files to distribute with install
              # List of tuples of (destFolder, [ local_files ])
              ('bin', [ 'bin/lgTaskProcessor' ])
          ],
          zip_safe=False,
          install_requires=[
              # -*- Extra requirements: -*-
          ],
          entry_points="""
          # -*- Entry points: -*-
          """
          )
