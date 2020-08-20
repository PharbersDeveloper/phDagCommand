import io
import setuptools
from glob import glob


with io.open("./README.md", "r", encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name="phcli",
    version="0.2.2",
    author="Alfred Yang",
    author_email="alfredyang@pharbers.com",
    maintainer="ClockQ",
    maintainer_email="zyqi@pharbers.com",
    description="pharbers dag scheduler config",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=2.7',

    package_data={
        '': ['template/*.yaml', 'template/*.tmp'],
    },
    data_files=[
        ('', glob('file/ph_data_clean/mapping_table/*')),
    ],
    entry_points={
        'console_scripts': [
            'phcli = phcli.__main__:phcli',
        ],
    }
)
