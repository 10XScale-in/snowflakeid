from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="snowflakeid",
    version="0.2.0",
    packages=find_packages(exclude=["tests*", "examples*", "config*"]),
    install_requires=[
        # Core dependencies (minimal for basic usage)
    ],
    extras_require={
        # Optional dependencies for enhanced features
        "redis": ["aioredis>=2.0.0"],
        "yaml": ["PyYAML>=5.1"],
        "consul": ["python-consul2>=0.1.5"],
        "all": ["aioredis>=2.0.0", "PyYAML>=5.1", "python-consul2>=0.1.5"],
    },
    author="Shudipto",
    author_email="shudipto@example.com",
    description="High-performance Snowflake ID generator with distributed coordination for Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/10XScale-in/snowflakeid",
    project_urls={
        "Bug Reports": "https://github.com/10XScale-in/snowflakeid/issues",
        "Source": "https://github.com/10XScale-in/snowflakeid",
        "Documentation": "https://github.com/10XScale-in/snowflakeid/blob/main/README.md",
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    keywords="snowflake id generator asyncio distributed microservices unique-id",
    zip_safe=False,
)
