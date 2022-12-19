from setuptools import setup

setup(name='ssebopLib',
      maintainer='Gabe Parrish, Steffi Kagone, Tony Butzer',
      maintainer_email='gbrlparrish@gmail.com',
      version='1.0.7',
      description='Classes and Functions for ssebop ws',
      packages=[
          'ssebopLib',
      ],
      install_requires=[
          'boto3',
      ],

)
