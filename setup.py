import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(name='fundcrunch',
      version='0.1.3',
      description='The Fundcrunch.Tech library for market data processing in real time or on history.',
      url='https://github.com/fundcrunch-tech/fundcrunch',
      author='Sergey Bryukov',
      author_email='sbryukov@gmail.com',
      long_description=long_description,
      long_description_content_type="text/markdown",
      license='MIT',
      packages=setuptools.find_packages(),#['drivers'],
      install_requires=[
          'requests', 'websockets', 'ccxt', 'pyzmq', 'pandas',
      ],
      classifiers=[
        "Programming Language :: Python :: 3",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",],
      zip_safe=False)
      