from setuptools import find_packages, setup
setup(
    name='py3x',
    version='0.1.4',
    description='python3 extension',
    url='https://github.com/uedak/py3x',
    author='uedak',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    include_package_data=True,
    packages=find_packages(),
    python_requires='>=3.6',
)
