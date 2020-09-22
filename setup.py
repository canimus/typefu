import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="typefu", # Replace with your own username
    version="0.0.1",
    author="Herminio Vazquez",
    author_email="canimus@gmail.com",
    description="A type casting guru from your data sources and data formats",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/canimus/typefu",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Topic :: Utilities"
    ],
    python_requires='>=3.6',
)