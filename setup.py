import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(name='fundcrunch',
      version='0.1.1',
      description='The Fundcrunch.Tech library for market data processing in real time or on history.',
      url='https://github.com/fundcrunch-tech/fundcrunch_py',
      author='Sergey Bryukov',
      author_email='hello@fundcrunch.tech',
      long_description=long_description,
      long_description_content_type="text/markdown",
      license='MIT',
      packages=setuptools.find_packages(),#['drivers'],
      install_requires=[
          'requests', 'websockets', 'ccxt', 'pyzmq',
      ],
      classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",],
      python_requires='>=3.7',
      zip_safe=False)
      