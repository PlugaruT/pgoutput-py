import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pgoutput-py",
    version="0.0.0",
    author="Tudor Plugaru",
    author_email="plugarutudor@gmail.com",
    description="PostgreSQL pgoutput decoder written in Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/PlugaruT/pgoutput-py",
    packages=setuptools.find_packages(where="pgoutput-py"),
    include_package_data=True,
    package_dir={"": "pgoutput-py"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=[
        "psycopg2-binary",
        "click"
    ],
    entry_points={
        'console_scripts': [
            'pgoutput-py = scripts:pgoutput_py',
        ],
    },
)
