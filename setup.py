import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(name='fundcrunch',
      version='0.1',
      description='The funniest joke in the world',
      url='http://github.com/storborg/funniest',
      author='Sergey Bryukov',
      author_email='flyingcircus@example.com',
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
      